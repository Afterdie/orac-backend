import time
import sqlglot
from sqlglot import parse, parse_one, expressions

def before_execute(conn, _clauseelement, _multiparams, _params):
    conn.info["query_start_time"] = time.time()

def after_execute(conn, clauseelement, _multiparams, _params, _result):
    #i should remove this later i think
    from main import QUERY_LOG  # Avoid circular imports at the top level

    elapsed = time.time() - conn.info["query_start_time"]
    query_text = str(clauseelement).strip().rstrip(";")  # Normalize query string

    # Split & normalize multiple queries
    try:
        queries = [q.sql() for q in parse(query_text)]
    except Exception as e:
        print(f"SQL Parsing Error: {e}")
        queries = [query_text]

    for query in queries:
        query_hash = hash(query)

        try:
            where_cols, join_cols, order_by_cols = extract_columns(query)
        except Exception as e:
            print(f"Failed to parse query columns: {e}")
            where_cols, join_cols, order_by_cols = [], [], []

        if query_hash not in QUERY_LOG:
            QUERY_LOG[query_hash] = {
                "query": query,
                "execution_time": elapsed,
                "frequency": 1,
                "where_columns": where_cols,
                "join_columns": join_cols,
                "order_by_columns": order_by_cols
            }
        else:
            QUERY_LOG[query_hash]["execution_time"] += elapsed
            QUERY_LOG[query_hash]["frequency"] += 1
        #debugging
        print(QUERY_LOG[query_hash])

def extract_columns(query):
    try:
        #keep in mind parse/parse_one fn returns a syntax tree
        parsed = parse_one(query)
    except Exception as e:
        print(f"SQL Parsing Error in extract_columns: {e}")
        return [], [], []

    where_columns = list({
        col.name for where in parsed.find_all(expressions.Where)
        for col in where.find_all(expressions.Column)
    })

    join_columns = list({
        col.name for join in parsed.find_all(expressions.Join)
        for col in join.find_all(expressions.Column)
    })

    order_by_columns = list({
        col.name for order in parsed.find_all(expressions.Order)
        for col in order.find_all(expressions.Column)
    })

    return where_columns, join_columns, order_by_columns
