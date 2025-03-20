from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text, Engine, event
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
from typing import Dict, AsyncGenerator, Optional, Tuple
import logging
from contextlib import asynccontextmanager
from docs import gen_docs
from logger import after_execute, before_execute


app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from schema import get_db_schema, TableSchema, TableStats

# Temporary database
SCHEMA_STORAGE: Dict[str, Tuple[Dict[str, TableSchema], Dict[str, TableStats]]] = {}
ENGINE_CACHE: Dict[str, Engine] = {}
QUERY_LOG = {}

# Function to get or create an engine
def get_engine(connection_string: str):
    if connection_string not in ENGINE_CACHE:
        engine = create_engine(connection_string, pool_size=5, max_overflow=10)
        event.listen(engine, "before_execute", before_execute)
        event.listen(engine, "after_execute", after_execute)
        ENGINE_CACHE[connection_string] = engine
    return ENGINE_CACHE[connection_string]

class ValidateRequest(BaseModel):
    connection_string: str

@app.post("/validate_connection/")
def validate_connection(request: ValidateRequest):
    try:
        engine = get_engine(request.connection_string)

        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))

            #includes the schema of the table and the extra stats
            schema = get_db_schema(engine)

            # Temporary solution (replace with Redis later)
            SCHEMA_STORAGE[request.connection_string] = schema

        return {"success": True, "schema": schema}
    except SQLAlchemyError as e:
        return {"success": False, "message": str(e)}

class QueryRequest(BaseModel):
    connection_string: str
    query: str

@app.post("/execute_query/")
def execute_query(request: QueryRequest):
    try:
        engine = get_engine(request.connection_string)

        with engine.connect() as connection:
            with connection.begin():  # Begin transaction for all queries
                result = connection.execute(text(request.query))

            if result.returns_rows:
                data = [dict(row) for row in result.mappings()]
                return {"success": True, "data": data}

        return {"success": True, "message": "Query executed successfully"}

    except SQLAlchemyError as e:
        return {"success": False, "message": str(e)}

#util function if you wish to acces this later
@app.post("/get_schema/")
def getschema(request: ValidateRequest):
    try:
        schema = SCHEMA_STORAGE[request.connection_string]
        return {"success":True, "schema": schema}
    except:
        return {"success": False, "message": "Failed to get schema"}
class DocsRequest(BaseModel):
    connection_string: Optional[str]
    db_schema: Optional[Dict[str, TableSchema]]

@app.post("/gen/docs")
def genDocs(request: DocsRequest):
    try:
        schema = SCHEMA_STORAGE[request.connection_string]
        return gen_docs(schema)
    except:
        return {"Success": False, "message":"Failed to generate docs"}

#need to test this what does bro even do
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    yield
    logging.info("Shutting down, closing all database connections...")
    for conn_str, engine in ENGINE_CACHE.items():
        logging.info(f"Closing connection for {conn_str}")
        engine.dispose()
    ENGINE_CACHE.clear()

app.router.lifespan_context = lifespan