import time
from sqlalchemy import text, Connection
from sqlalchemy.exc import SQLAlchemyError

def execute_query(connection: Connection, query: str):
    try:
        with connection.begin():
            start_time = time.perf_counter()
            result = connection.execute(text(query))
            duration = time.perf_counter() - start_time

            if result.returns_rows:
                data = [dict(row) for row in result.mappings()]
                return {"success": True, "data": data, "duration": duration}

            return {"success": True, "message": "Query executed successfully", "duration": duration}

    except SQLAlchemyError as e:
        return {"success": False, "message": str(e)}