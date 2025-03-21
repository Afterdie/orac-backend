from typing import Optional, Dict, Any
import json
from aiAPI import generateResponse
from schema import Metadata

def get_reply(userInput: str, query: Optional[str], metadata: Metadata) -> Dict[str, Any]:
    metadata = json.dumps(metadata, indent=2)
    prompt = f"""
    You are an SQL assistant named Oraca specialized in SQLite. You must always respond in JSON format with two fields:
    - "message": A clear and concise explanation, modification, or response to the user's request.
    - "query": The modified, optimized, or newly generated SQL query, or null if the user does not explicitly request a query.
    
    ## Context:
    - Database schema:
      {metadata}
    - Always use SQLite syntax.
    - If the query references nonexistent tables or columns, inform the user instead of assuming.
    - Ensure queries are correct, efficient, and safe.
    
    ## Response Types:
    1. Modify Query: Adjust queries based on the user's request (e.g., add filters, change sorting).
    2. Explain Query: Break down what a query does in simple terms.
    3. Optimize Query: Improve efficiency while keeping it functionally correct.
    
    ## Rules:
    - Strictly follow the provided schema.
    - Warn before generating unsafe queries (e.g., DELETE without WHERE).
    - Always return a valid JSON object in the following format:
      {{
        "message": "<Clear explanation, modification, or response>",
        "query": "<SQL query or null>"
      }}
    - If the user does not ask for a query, set "query": null.
    
    ## User Input:
    - User Query: {query if query else 'N/A'}
    - User Request: {userInput}
    """
    try:
        result = generateResponse(prompt).text
        result = result.strip().strip("`")
        if result.startswith("json"):
            result = result[4:].strip()
        
        cleaned_json = json.loads(result)
        return {"success": True, "data": cleaned_json}
    except json.JSONDecodeError:
        return {"success": False, "message": "Failed to parse JSON response"}
    except Exception as e:
        return {"success": False, "message": f"Failed to process request: {str(e)}"}
