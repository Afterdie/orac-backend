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

from schema import get_schema
from schema import TableSchema

@app.post("/get_schema/")
def getschema(request: ValidateRequest):
    return get_schema(request.connection_string)

from docs import gen_docs
class DocsRequest(BaseModel):
    connection_string: Optional[str]
    db_schema: Optional[Dict[str, TableSchema]]

@app.post("/gen/docs")
def genDocs(request: DocsRequest):
    print(request)
    return gen_docs(request.connection_string, request.db_schema)