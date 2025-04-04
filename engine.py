from typing import Dict
from sqlalchemy import create_engine, text, Engine, inspect, event
from sqlalchemy.exc import SQLAlchemyError
from schema import Metadata
from logger import after_execute, before_execute

# Temporary database
ENGINE_CACHE: Dict[str, Engine] = {}
METADATA_STORAGE: Dict[str, Metadata] = {}

def validate_connection(connection_string: str):
    try:
        engine = get_engine(connection_string)

        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))

            #includes the schema of the table and the extra stats
            metadata = get_db_metadata(connection_string)
            METADATA_STORAGE[connection_string] = metadata
            #binding after schema because it runs a huge query and it sends through a wall of text QOL change 
            event.listen(engine, "before_execute", before_execute)
            event.listen(engine, "after_execute", after_execute)

        return {"success": True, "data": metadata}
    except SQLAlchemyError as e:
        return {"success": False, "message": str(e)}

# Function to get or create an engine
def get_engine(connection_string: str):
    if connection_string not in ENGINE_CACHE:
        engine = create_engine(connection_string, pool_size=5, max_overflow=10)
        ENGINE_CACHE[connection_string] = engine
    return ENGINE_CACHE[connection_string]

# #need to test this what does bro even do
# @asynccontextmanager
# async def lifespan(app: FastAPI) -> AsyncGenerator:
#     yield
#     logging.info("Shutting down, closing all database connections...")
#     for conn_str, engine in ENGINE_CACHE.items():
#         logging.info(f"Closing connection for {conn_str}")
#         engine.dispose()
#     ENGINE_CACHE.clear()

# app.router.lifespan_context = lifespan


#metadata is schema + stats
def get_db_metadata(connection_string: str) -> Metadata:

    if connection_string in METADATA_STORAGE:
        return METADATA_STORAGE.get(connection_string)
    
    engine = get_engine(connection_string)
    inspector = inspect(engine)
    schema = {}
    stats = {}

    for table in inspector.get_table_names():
        schema[table] = {
            "columns": [],
            "foreign_keys": [],
            "relationships": [],
            "indexes": [],
        }

        # Extract columns
        for col in inspector.get_columns(table):
            schema[table]["columns"].append({
                "name": col["name"],
                "type": str(col["type"]),
                "nullable": col["nullable"]
            })

        # Extract foreign keys & relationships
        for fk in inspector.get_foreign_keys(table):
            relationship = {
                "from_table": table,
                "from_columns": fk["constrained_columns"],
                "to_table": fk["referred_table"],
                "to_columns": fk["referred_columns"]
            }
            schema[table]["foreign_keys"].append({
                "column": fk["constrained_columns"],
                "references_table": fk["referred_table"],
                "referenced_column": fk["referred_columns"]
            })
            schema[table]["relationships"].append(relationship)

        # Extract indexes
        for index in inspector.get_indexes(table):
            schema[table]["indexes"].append({
                "name": index["name"],
                "columns": index["column_names"],
                "unique": index.get("unique", False)  # Some DBs might not have 'unique' field
            })

        # Fetch row count & cardinality
        stats[table] = get_stats(engine, table)

    return {"local_schema": schema, "stats":stats}


def get_stats(engine, table_name):
    stats = {"row_count": 0, "cardinality": {}}

    with engine.connect() as conn:
        try:
            # Get row count
            row_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            stats["row_count"] = row_count

            # Get cardinality for each column
            inspector = inspect(engine)
            columns = [col["name"] for col in inspector.get_columns(table_name)]

            for col in columns:
                unique_count = conn.execute(text(f"SELECT COUNT(DISTINCT {col}) FROM {table_name}")).scalar()
                stats["cardinality"][col] = unique_count / row_count if row_count else 0

        except Exception as e:
            print(f"Error fetching stats for {table_name}: {e}")

    return stats