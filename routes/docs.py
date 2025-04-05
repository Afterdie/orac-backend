from typing import Dict
import json
from utils.schema import TableSchema
from utils.aiAPI import generateResponse

#schema_types = get_types()
#available block types
block_note_block_types = [
    {
        "type": "table",
        "content": {
            "type": "tableContent",
            "rows": [
                {"cells": ["Column Names", "DataType"]},
                {"cells": ["sampletable", "TEXT"]},
            ],
        },
    },
    {"type": "heading", "content": "", "props": {"level": 1}},
    {"type": "heading", "content": "", "props": {"level": 2}},
    {"type": "heading", "content": "", "props": {"level": 3}},
    {"type": "bulletListItem", "content": ""},
    {"type": "bulletListItem", "content": ""},
    {"type": "paragraph", "content": ""},
    {
        "type": "codeBlock",
        "content": "SELECT * FROM table",
        "props": {"language": "sql"},
    },
]

def gen_docs(schema: Dict[str, TableSchema]):
    prompt = f"""
    You are an AI specialized in generating structured database documentation in strict compliance with the BlockNote block format. Your task is to produce industry-standard documentation explaining all fields of the table and general information, while adhering to the provided database schema, and explicitly allowed block types. Also include a few sample queries in the code block for each table and write the explanations for these queries in a paragraph block. When you wish to add a gap between two topics and two SQL queries, simply use an empty paragraph block.

    Rules & Constraints
    - Output must be a valid JSON object strictly following the BlockNote block format.
    - Do not generate or include any block types other than those explicitly provided.
    - Do not introduce extra fields beyond those defined in the database schema.
    - Do not introduce extra data types beyond those defined in the supported data types.
    - Maintain readability and logical structuring while staying within BlockNote's JSON format.
    - Your response must contain JSON only.

    Provided Information
    Database Schema
    {schema}

    Allowed Block Types (Use only these, no others)
    {block_note_block_types}

    Important: If a required structure cannot be represented using the allowed block types, do not attempt to create new block types—strictly use only what is provided. If a concept cannot be documented using the available blocks, omit it instead of introducing new ones.
    """

    try:
        result = generateResponse(prompt).text
        # Clean the response by removing markdown code block markers you must remove the newline characters or they obstruct backtick removal
        result = result.strip().strip("`")

        if result.startswith("json"):
            result = result[4:].strip()
        
        cleaned_json = json.loads(result)
        return {"success": True, "data": cleaned_json["blocks"]}

    except json.JSONDecodeError:
        return {"success": False, "message": "Failed to parse JSON response"}
    except Exception as e:
        return {"success": False, "message": f"Failed to generate docs: {str(e)}"}
