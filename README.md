# oraca-backend

## Getting Started

Preferably create a virtual environment.
```
cd oraca-backend
venv/Scripts/Activate
```

## Add your Gemini API key
Create a .env in the root of the project and add the following fiels
```
GEMINI_API_KEY=your_api_key_here
```

## Installing dependencies
```
pip install -r requirements.txt
```

## Start server
```
uvicorn main:app --reload
```

# Routes
## /validate_connection
Client passes a connection string for the database that is verified by the server by running a dummy query
```
connection_string: str
```

## /execute_query
Accepts the query to be executed on the db.
```
connection_string: str
query: str
```

## /get_schema
Utility function gets the metadata(schema+stats) mapped to the connection_string.
```
connection_string: str
```

## /nlp2sql
Accepts the natural language effect they desire on the db, connection_string if online execution or schema if local db.
```
description: str
connection_string: Optional[str]
schema: Optional[Dict[str, TableSchema]]
```

## /docs
Accepts the connection_string if online connection or the schema if local db
```
connection_string: Optional[str]
schema: Optional[Dict[str, TableSchema]]
```

## /chat
Accepts user prompt and query if included and returns a response based on the metadata provided to it.
```
userInput: str
query: Optional[str]
connection_string: Optional[str]
metadata: Optional[Metadata]
```

## /graph
Accepts the user prompt and query if included and returns a response that included the graph type and the data for it based on the metadata and prompt provided to it.
```
userInput: str
query: Optional[str]
connection_string: Optional[str]
metadata: Optional[Metadata]
```

fields used for index decision making

- query execution time
- frequently used query
- columns used in where join order by - which column needs index

- row count if very low no advantage of creating index
- should have high cardinality (original values) more than 50% - which column can be indexed

steps
- sort by highest query time
- most ocurring query
- get column names near where
- do these column have high row count ? do these columns have high cardinality

- suggest index creation

select * employees : 8
