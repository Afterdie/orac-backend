from sqlalchemy import create_engine, inspect, text, Engine
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
from typing import List, Dict


class ColumnSchema(BaseModel):
    name: str
    type: str
    nullable: bool

class ForeignKeySchema(BaseModel):
    column: List[str]
    references_table: str
    referenced_column: List[str]

class RelationshipSchema(BaseModel):
    from_table: str
    from_columns: List[str]
    to_table: str
    to_columns: List[str]

class IndexSchema(BaseModel):
    name: str
    columns: List[str]
    unique: bool

class TableSchema(BaseModel):
    columns: List[ColumnSchema]
    foreign_keys: List[ForeignKeySchema]
    relationships: List[RelationshipSchema]
    indexes: List[IndexSchema]

class TableStats(BaseModel):
    row_count: int
    cardinality: Dict[str, float]

class Metadata(BaseModel):
    schema: Dict[str, TableSchema]
    stats: Dict[str, TableStats]

# Separate utility function
def get_schema(connection_string: str):
    if not connection_string:
        return {"success": False, "message": "Invalid connection string"}
    try:
        engine = create_engine(connection_string)
        metadata = get_db_metadata(engine)
        return {"success": True, "data": metadata}
    except SQLAlchemyError as e:
        return {"success": False, "message": str(e)}

#metadata is schema + stats
def get_db_metadata(engine) -> Metadata:
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

    return schema, stats


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