from typing import Dict
from utils.schema import TableSchema
from utils.aiAPI import generateResponse
from routes.execute import execute_query
# fallback method to ensure the returned query is syntactically correct
def is_select_query(query: str) -> bool:
    return query.strip().lower().startswith("select")

def wrap_in_safe_subquery(query: str) -> str:
    query = query.strip().rstrip(";")
    return f"SELECT * FROM (\n{query}\n) AS sub WHERE 1=2"

def verify_query(connection_string: str, query: str):
    if not query or not isinstance(query, str):
        return {"success": False, "message": "Invalid query passed for verification."}

    # Wrap only if it's a SELECT query
    if is_select_query(query):
        safe_query = wrap_in_safe_subquery(query)
    else:
        safe_query = query  # Let it run as-is, probably will fail if invalid

    validity = execute_query(connection_string, safe_query)

    if validity.get("success") is True:
        return {"success": True, "data": query}

    # Fall back to GPT correction if query failed
    prompt = f"""
    You are an AI specialized in correcting SQL queries.

    Rules:
    - Use standard SQL syntax
    - ONLY return a valid SQL query. No comments, markdown, or explanation.
    - Fix syntax ONLY — do not alter table or column names.

    Input SQL:
    {query}
    """

    try:
        result = generateResponse(prompt).text.strip().strip("`")

        if result.lower().startswith("sql"):
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
        
        return verify_query(connection_string, result)

    except Exception as e:
        return {"success": False, "message": f"Failed to generate SQL: {str(e)}"}
