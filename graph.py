from typing import Optional, Dict, Any
import json
from aiAPI import generateResponse
from schema import Metadata
from execute import execute_query
from sqlalchemy import Connection

class GraphResponse:
    message: str
    query: str
    type: str

def get_graph(userInput: str, query: Optional[str], metadata: Metadata, connection_string: str) -> Dict[str, Any]:
    metadata = json.dumps(metadata, indent=2)
    structure = {
        "message": "<Brief explanation of the query>",
        "query": "<Generated SQL query>",
        "type": "<Graph type: pie, line, bar, radial, area, table>"
    }
    sampledata = [
        {
            "month": "January", "desktop": 186, "mobile": 80, "tablet": 50 
        },
        {
            "month": "February", "desktop": 305, "mobile": 200, "tablet": 90
        }
    ]
    prompt = f"""You are an intelligent SQL visualization assistant named Oraca. Your role is to generate an SQL query that retrieves structured data in the correct format for visualization. You must also determine the most appropriate chart type based on the database schema provided below and user input.

    Context:
    Database Schema:
    {metadata}

    **VERY IMPORTANT**:Your response MUST always be in JSON format with the following structure and use double quotes:
    {structure}

    Your Responsibilities:
    Generate an optimized SQL query that returns structured data suitable for visualization.
    Perform necessary data manipulation (e.g., COUNT, SUM, AVG, GROUP BY, ORDER BY) to format the data appropriately for the requested visualization.
    Choose the most appropriate chart type based on the user request and schema.
    If a requested table or column does not exist, inform the user instead of assuming.
    Chart Selection Rules:

    1. Pie Chart (For Categorical Aggregation)
    The query must return exactly 2 columns, formatted as:
    First column: Label (categorical, e.g., category, product name, region).
    Second column: Value (aggregated, e.g., count, sum, percentage).
    Example Use Case: "Show sales distribution by category."

    2. Bar & Line Charts (For Trends and Comparisons)
    The query must return one x-axis column first, followed by up to 3 y-axis columns (metrics).
    The additional 3 metrics should provide meaningful comparisons to the main metric (e.g., comparing mobile, desktop, and tablet usage over time).
    Example Use Case: "Show monthly revenue for different sales channels."

    3. Radial Chart (For Circular Progress Data)
    The query must return exactly 2 columns, where the second column represents a percentage, score, or count.
    Example Use Case: "Show completion rates per department."

    4. Area Chart (For Trend Visualization with Shading)
    Similar to bar and line charts but with shaded regions to indicate volume.
    Example Use Case: "Show cumulative sales over time with trend analysis."

    5. Fallback to Table Format (If no suitable visualization type is found).

    Query Constraints:
    The x-axis column must always be the first column in the result.
    Up to 3 additional y-axis metrics are allowed (for meaningful comparisons).
    Ensure readability by renaming columns (e.g., total_orders → Orders).

    Expected Data Format Example:
    For a bar chart displaying monthly device usage, the query should return structured data like this:
    {sampledata}
    "month" is the x-axis column.

    "desktop", "mobile", and "tablet" are y-axis metrics for comparison.

    User Request: {userInput}"""
    try:
        result = generateResponse(prompt).text
        result = result.strip().strip("`")
        if result.startswith("json"):
            result = result[4:].strip()
        #print("FIRST RES",result)
        cleaned_json = json.loads(result)
        message = cleaned_json["message"]
        query = cleaned_json["query"]
        type = cleaned_json["type"]

        rows = execute_query(connection_string, query)
        #print("ROWS:",rows)
        if rows["success"]:
            data = {
                "message": message,
                "graph": type,
                "chartData": rows["data"]
            }
            #print(data)
            return {"success": True, "data": data}
        else: 
            return {"success": False, "message": "Something went wrong"}
        
    except json.JSONDecodeError:
        return {"success": False, "message": "Failed to parse JSON response"}
    except Exception as e:
        return {"success": False, "message": f"Failed to process request: {str(e)}"}
