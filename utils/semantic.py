import hashlib
from typing import Dict, List
import numpy as np
from sentence_transformers import SentenceTransformer
from sqlalchemy import text

from utils.engine import get_engine, get_db_metadata
from utils.schema import Metadata

#chatgpt suggest hashing the connection str which makes sense implement that everywhere else

class EmbeddingStore:
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)
        # Structure: {conn_hash: { "table.col": {value_hash: {value, embedding}}}}
        self.cache: Dict[str, Dict[str, Dict[str, Dict[str, np.ndarray]]]] = {}

    def _hash(self, text: str) -> str:
        return hashlib.sha256(text.encode()).hexdigest()

    def _conn_key(self, connection_string: str) -> str:
        return self._hash(connection_string)

    def add_value(self, connection_string: str, table: str, column: str, value: str):
        conn_key = self._conn_key(connection_string)
        col_key = f"{table}.{column}"
        value_hash = self._hash(value)

        self.cache.setdefault(conn_key, {})
        self.cache[conn_key].setdefault(col_key, {})
        if value_hash not in self.cache[conn_key][col_key]:
            embedding = self.model.encode(value)
            self.cache[conn_key][col_key][value_hash] = {
                "value": value,
                "embedding": embedding
            }

    # gets the embeddings for given column
    def get_embeddings(self, connection_string: str, table: str, column: str) -> List[Dict]:
        conn_key = self._conn_key(connection_string)
        col_key = f"{table}.{column}"
        return list(self.cache.get(conn_key, {}).get(col_key, {}).values())

    def similarity_search(self, connection_string: str, table: str, column: str, query: str, threshold: float = 0.8) -> str:
        query_vec = self.model.encode(query)
        candidates = self.get_embeddings(connection_string, table, column)

        if not candidates:
            return query  # fallback

        def cosine(a, b):
            return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

        best_score = -1
        best_match = query

        for entry in candidates:
            score = cosine(query_vec, entry["embedding"])
            if score > best_score:
                best_score = score
                best_match = entry["value"]

        return best_match if best_score >= threshold else query

    # accept the connection string, metadata for getting the row count and cardinality to determine whether worth storing or not, 

    #gets the row count and sets a limit dynamically
    def generate_from_metadata(self, connection_string: str, cardinality_threshold: float = 0.6):
        metadata = get_db_metadata(connection_string)
        engine = get_engine(connection_string)

        #handle this better raise a http exception llater
        if not metadata or not engine:
            return
        
        schema = metadata.get("local_schema")
        stats = metadata.get("stats")

        #handle this better also 
        if not schema or not stats:
            return 

        for table, table_data in schema.items():
            for col in table_data["columns"]:
                col_name = col["name"]
                col_type = col["type"].lower()

                if not self._is_text_type(col_type):
                    continue

                col_stats = stats.get(table, {})
                cardinality = col_stats.get("cardinality", {}).get(col_name, 1.0)
                row_count = col_stats.get("row_count", 0)

                if cardinality > cardinality_threshold:
                    continue

                # Dynamically set limit: allow 5% of rows or max 500
                limit = min(500, max(10, int(row_count * cardinality)))

                with engine.connect() as conn:
                    try:
                        result = conn.execute(text(
                            f"SELECT DISTINCT {col_name} FROM {table} WHERE {col_name} IS NOT NULL LIMIT {limit}"
                        ))
                        for row in result:
                            value = str(row[0])
                            self.add_value(connection_string, table, col_name, value)
                    except Exception as e:
                        print(f"[embedding] Failed {table}.{col_name}: {e}")


    def _is_text_type(self, col_type: str) -> bool:
        return any(t in col_type for t in ["text", "char", "varchar", "string"])
