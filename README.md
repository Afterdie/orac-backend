# orac-backend

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
