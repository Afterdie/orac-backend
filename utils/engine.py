from typing import Dict
from sqlalchemy import create_engine, text, Engine, inspect, event
from sqlalchemy.exc import SQLAlchemyError
from utils.schema import Metadata
from utils.logger import after_execute, before_execute
import time
from utils.semantic import EmbeddingStore


# Temporary database
ENGINE_CACHE: Dict[str, Engine] = {}
METADATA_STORAGE: Dict[str, Metadata] = {}

def validate_connection(connection_string: str):
    start_time = time.perf_counter()
    store = EmbeddingStore.get_instance()
    try:
        engine = get_engine(connection_string)

        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))

            #includes the schema of the table and the extra stats
            metadata = get_db_metadata(connection_string)
            METADATA_STORAGE[connection_string] = metadata
            
            # pass the connection string to create embeddings, cardinality threhold decides which columns get embeddings
            # 500 embeddings for a row is ok amount to get the embedding count simply multiply the cardinality with the row count so 0.05*10000 would mean 500 values

            # higher threshold means no correction for majority of the columns and too low and you make the application slow and overflow the memory since stored in the cache
            store.generate_embeddings(engine, connection_string, metadata, 0.4)
            # developmental
            # store.printCache()

            #binding after schema because it runs a huge query and it sends through a wall of text QOL change 
            event.listen(engine, "before_execute", before_execute)
            event.listen(engine, "after_execute", after_execute)
            print(time.perf_counter()-start_time)
        return {"success": True, "data": metadata}
    except SQLAlchemyError as e:
        return {"success": False, "message": str(e)}

# Function to get or create an engine
def get_engine(connection_string: str):
    if connection_string not in ENGINE_CACHE:
        engine = create_engine(connection_string, pool_size=5, max_overflow=10)
        ENGINE_CACHE[connection_string] = engine
    return ENGINE_CACHE[connection_string]

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

def dispose_all_engines():
    for _, engine in ENGINE_CACHE.items():
        engine.dispose()
    ENGINE_CACHE.clear()