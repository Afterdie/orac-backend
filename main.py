from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text, Engine, event
from sqlalchemy.exc import SQLAlchemyError
from pydantic import BaseModel
from typing import Dict, AsyncGenerator, Optional
import logging
from contextlib import asynccontextmanager

from docs import gen_docs
from logger import after_execute, before_execute
from chat import get_reply
from schema import get_db_metadata, Metadata
import json


app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Temporary database
METADATA_STORAGE: Dict[str, Metadata] = {}
ENGINE_CACHE: Dict[str, Engine] = {}
QUERY_LOG = {}

# Function to get or create an engine
def get_engine(connection_string: str):
    if connection_string not in ENGINE_CACHE:
        engine = create_engine(connection_string, pool_size=5, max_overflow=10)
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
            metadata = get_db_metadata(engine)

            # Temporary solution (replace with Redis later)
            METADATA_STORAGE[request.connection_string] = metadata
            #binding after schema because it runs a huge query and it sends through a wall of text QOL change 
            event.listen(engine, "before_execute", before_execute)
            event.listen(engine, "after_execute", after_execute)

        return {"success": True, "data": metadata}
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
        metadata = METADATA_STORAGE[request.connection_string]
        return {"success":True, "data": metadata.schema}
    except:
        return {"success": False, "message": "Failed to get schema"}
class DocsRequest(BaseModel):
    connection_string: Optional[str]
    db_schema: Optional[Metadata]

@app.post("/gen/docs")
def genDocs(request: DocsRequest):
    try:
        metadata = METADATA_STORAGE[request.connection_string]
        return gen_docs(metadata.schema)
    except:
        return {"Success": False, "message":"Failed to generate docs"}

class ChatRequest(BaseModel):
    userInput: str
    query: Optional[str]
    connection_string: Optional[str]
    metadata: Optional[Metadata]

@app.post("/chat")
def getReply(request: ChatRequest):
    userInput = request.userInput
    query = request.query
    connection_string = request.connection_string
    metadata = request.metadata
    if metadata:
        #done because its not json serilizable by default and contains pydantic models
        metadata = metadata.model_dump()
    if not connection_string and not metadata:
        return {"success": False, "message":"Not enough data"}
    return get_reply(userInput, query, metadata or METADATA_STORAGE.get(connection_string))

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