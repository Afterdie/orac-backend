from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, Optional
import os
from dotenv import load_dotenv

from utils.schema import Metadata, TableSchema
from utils.engine import validate_connection, dispose_all_engines, get_db_metadata

from routes.execute import execute_query
from routes.nlp2sql import get_sql
from routes.docs import gen_docs
from routes.chat import get_reply
from routes.graph import get_graph

load_dotenv()
DEV_MODE = os.getenv("DEV_MODE", "false").lower() == "true"
def dev_print(*args, **kwargs):
    if DEV_MODE:
        print(*args, **kwargs)

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ValidateRequest(BaseModel):
    connection_string: str

@app.get("/health")
def health():
    return {"message":"working and stuff"}

@app.post("/validate_connection")
async def validateConnection(request: ValidateRequest):

    connection_string = request.connection_string
    if not connection_string:
        return {"success": False, "message":"Connection string is empty"}
    
    return validate_connection(connection_string)

class QueryRequest(BaseModel):
    connection_string: str
    query: str

#move this to its own file later
@app.post("/execute_query")
async def executeQuery(request: QueryRequest):
    connection_string = request.connection_string
    query = request.query

    if not connection_string or not query:
        return {"success": False, "message": "Connection string or Query is missing"}

    return execute_query(connection_string, query)

class NLPRequest(BaseModel):
    description: str
    connection_string: Optional[str]
    local_schema: Optional[Dict[str, TableSchema]]

@app.post("/nlp2sql")
async def getSQL(request: NLPRequest):
    description = request.description
    connection_string = request.connection_string
    schema = request.local_schema
    if not schema:
        schema = get_db_metadata(connection_string).get("local_schema")
    return get_sql(description, schema, connection_string)

class DocsRequest(BaseModel):
    connection_string: Optional[str]
    local_schema: Optional[Dict[str, TableSchema]]

@app.post("/docs")
async def genDocs(request: DocsRequest):
    connection_string = request.connection_string
    schema = request.local_schema
    if not connection_string and not schema:
        return {"success": False, "message": "Field connection_string or schema is missing"}
    #need some better edge case handling here in case metadata.get() returns None
    return gen_docs(schema or get_db_metadata(connection_string).get("local_schema"))

class ChatRequest(BaseModel):
    userInput: str
    query: Optional[str]
    connection_string: Optional[str]
    metadata: Optional[Metadata]

@app.post("/chat")
async def getReply(request: ChatRequest):
    userInput = request.userInput
    query = request.query
    connection_string = request.connection_string
    metadata = request.metadata
    if metadata:
        #done because its not json serilizable by default and contains pydantic models
        metadata = metadata.model_dump()
    if not connection_string and not metadata:
        return {"success": False, "message":"Not enough data"}
    return get_reply(userInput, query, metadata or get_db_metadata(connection_string))

@app.post("/graph")
async def getGraph(request: ChatRequest):
    userInput = request.userInput
    query = request.query
    connection_string = request.connection_string
    metadata = request.metadata
    if metadata:
        #done because its not json serilizable by default and contains pydantic models
        metadata = metadata.model_dump()
    if not connection_string and not metadata:
        return {"success": False, "message":"Not enough data"}
    return get_graph(userInput, query, metadata or get_db_metadata(connection_string), connection_string)

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    yield
    try:
        logging.info("Shutting down, closing all database connections...")
        dispose_all_engines()
    except Exception as e:
        logging.exception("Failed during shutdown: %s", str(e))

app.router.lifespan_context = lifespan