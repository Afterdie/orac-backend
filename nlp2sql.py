from typing import Dict
from schema import TableSchema
from aiAPI import generateResponse

def get_sql(description: str, schema: Dict[str, TableSchema]):
    prompt = f"""
    You are an AI specialized in converting natural language to strict SQLite queries.

    Database Schema:
    {schema}

    Rules:
    - Follow the schema exactly.
    - Use only valid SQLite syntax.
    - Do not put formatting backticks or language name in the response.

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
