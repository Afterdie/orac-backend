import time
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from engine import get_engine

def execute_query(connection_string: str, query: str):
    try:
        engine = get_engine(connection_string)
        with engine.connect() as connection:  # Ensure proper connection handling
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