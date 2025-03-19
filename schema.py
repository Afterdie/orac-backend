from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
from typing import List, Dict, Optional

def get_schema(connection_string:str):
    if not connection_string:
        return {"success":False, "message": "Invalid connection string"}
    try:
        engine = create_engine(connection_string)
        schema = get_db_schema(engine)
        return {"success": True, "schema": schema}
    except SQLAlchemyError as e:
        return {"success": False, "message": str(e)}

def get_db_schema(engine):
    inspector = inspect(engine)
    schema = {}

    for table in inspector.get_table_names():
        schema[table] = {
            "columns": [],
            "foreign_keys": [],
            "relationships": []
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

    return schema


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

class TableSchema(BaseModel):
    columns: List[ColumnSchema]
    foreign_keys: List[ForeignKeySchema]
    relationships: List[RelationshipSchema]
