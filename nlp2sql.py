from typing import Dict
from schema import TableSchema
from aiAPI import generateResponse
from execute import execute_query
# fallback method to ensure the returned query is syntactically correct
def verify_query(connection_string: str, query: str):
    paddedQuery = query+" AND 1=2"
    print(paddedQuery)
    validity = execute_query(connection_string, paddedQuery)
    print(validity)
    if validity["success"] is True:
        #query is valid
        return { "success": True, "data": query}
    
    prompt = f"""
    You are an AI specialized in correcting sql queries provided you.
    Input Query:
    {query}

    Make sure the provided query has no syntax errors and return a valid SQL query. Only fix syntax do not attempt to correct the names of tables, conditions or other variables.
    """
    try:
        result = generateResponse(prompt).text
        result = result.strip().strip("`")

        if result.startswith("sql"):
            result = result[4:].strip()
        
        return {"success": True, "data": result}

    except Exception as e:
        return {"success": False, "message": f"Failed to generate SQL: {str(e)}"}

def get_sql(description: str, schema: Dict[str, TableSchema], connection_string: str):
    prompt = f"""
    You are an AI specialized in converting natural language to strict SQL queries.

    Database Schema:
    {schema}

    Rule:
    - Follow the schema exactly.

    Example Query:
    User Input: Show me all users who placed an order over $50.
    SQL Output:
    SELECT users.id, users.name FROM users JOIN orders ON users.id = orders.user_id WHERE orders.amount > 50;

    User Input: {description}
    Generate SQL Output for this following the given rules and DO NOT PUT FORMATTING ON THE ANSWER.
    """

    try:
        result = generateResponse(prompt).text
        result = result.strip().strip("`")

        if result.startswith("sql"):
            result = result[4:].strip()
        
        return {"success": True, "data": result}

    except Exception as e:
        return {"success": False, "message": f"Failed to generate SQL: {str(e)}"}
