import time
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from utils.engine import get_engine
from utils.semantic import EmbeddingStore
import sqlglot
from sqlglot.expressions import Where, EQ, Column, Literal

def patch_query_with_semantics(connection_string: str, query: str) -> str:
    store = EmbeddingStore.get_instance()
    statements = sqlglot.parse(query)  # Handles multiple queries

    for ast in statements:
        for where in ast.find_all(Where):
            for expr in where.find_all(EQ):
                col_expr = expr.args.get("this")
                val_expr = expr.args.get("expression")

                if not isinstance(col_expr, Column) or not isinstance(val_expr, Literal):
                    continue

                table = col_expr.table or "__default__"
                column = col_expr.name
                value = val_expr.this

                if not isinstance(value, str):
                    continue
                print(store.printCache())
                print(store.has_value(connection_string, table, column, value))
                # Check semantic cache
                if not store.has_value(connection_string, table, column, value):
                    new_val = store.semantic_search(connection_string, table, column, value)
                    expr.set("expression", Literal.string(new_val))

    # Recombine all modified statements
    return ";\n".join(ast.sql() for ast in statements)


def execute_query(connection_string: str, query: str):
    try:
        engine = get_engine(connection_string)
        print(query)
        # Patch all statements
        patched_query = patch_query_with_semantics(connection_string, query)
        print(patched_query)
        with engine.connect() as connection:
            with connection.begin():
                start_time = time.perf_counter()
                result = connection.execute(text(patched_query))
                duration = time.perf_counter() - start_time

                if result.returns_rows:
                    data = [dict(row) for row in result.mappings()]
                    return {"success": True, "data": data, "duration": duration}

                return {"success": True, "message": "Query executed successfully", "duration": duration}

    except SQLAlchemyError as e:
        return {"success": False, "message": str(e)}
