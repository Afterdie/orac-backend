from fastapi import FastAPI

#dev
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
from typing import List, Dict, Optional

app = FastAPI()

#classic cors error
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],)

class ValidateRequest(BaseModel):
    connection_string: str

@app.post("/validate_connection/")
def validate_connection(request: ValidateRequest):
    try:
        engine = create_engine(request.connection_string)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return {"success": True, "message": "Connection is valid"}
    except SQLAlchemyError as e:
        return {"success": False, "message": str(e)}

class QueryRequest(BaseModel):
    connection_string: str
    query: str

@app.post("/execute_query/")
def execute_query(request: QueryRequest):
    try:
        engine = create_engine(request.connection_string)

        with engine.connect() as connection:
            with connection.begin():  # Begin transaction for all queries
                result = connection.execute(text(request.query))

            if result.returns_rows:
                data = [dict(row) for row in result.mappings()]
                return {"success": True, "data": data}

        return {"success": True, "message": "Query executed successfully"}

    except SQLAlchemyError as e:
        return {"success": False, "message": str(e)}

@app.post("/get_schema/")
def get_schema(request: ValidateRequest):
    try:
        engine = create_engine(request.connection_string)
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

class DocsRequest(BaseModel):
    connection_string: str
    schema: Optional[Dict[str, TableSchema]] = None

# from docs import generateDocs

# @app.post("/gen/docs")
# def genDocs(request: DocsRequest):
#     try:
#         generateDocs(request)    