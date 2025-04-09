import hashlib
from typing import Dict, List
import numpy as np
from sentence_transformers import SentenceTransformer
from sqlalchemy import text, Engine

from utils.schema import Metadata
import logging
import os
from dotenv import load_dotenv
load_dotenv()
class EmbeddingStore:
    _instance = None

    def __init__(self):
        if EmbeddingStore._instance is not None:
            raise Exception("Use EmbeddingStore.get_instance() to access the singleton.")
        logging.basicConfig(level=logging.DEBUG)
        model_path = os.path.join(os.getcwd(), "models", "all-MiniLM-L6-v2")
        self.model = SentenceTransformer(model_path)
        # Structure: {conn_hash: { "table.col": {value_hash: {value, embedding}}}}
        self.cache: Dict[str, Dict[str, Dict[str, Dict[str, np.ndarray]]]] = {}

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = EmbeddingStore()
        return cls._instance

    def _hash(self, text: str) -> str:
        return hashlib.sha256(text.encode()).hexdigest()

    def _conn_key(self, connection_string: str) -> str:
        return self._hash(connection_string)

    def has_value(self, connection_str: str, table: str, column: str, value: str) -> bool:
        conn_key = self._conn_key(connection_str)
        col_key = f"{table}.{column}"
        value_hash = self._hash(value)

        return (
            conn_key in self.cache and
            col_key in self.cache[conn_key] and
            value_hash in self.cache[conn_key][col_key]
        )

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

    def get_embeddings(self, connection_string: str, table: str, column: str) -> List[Dict]:
        conn_key = self._conn_key(connection_string)
        col_key = f"{table}.{column}"
        return list(self.cache.get(conn_key, {}).get(col_key, {}).values())

    def semantic_search(self, connection_string: str, table: str, column: str, query: str, threshold: float = 0) -> str:
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

    def generate_embeddings(self, engine: Engine, connection_string: str, metadata: Metadata, cardinality_threshold: float = 0.6):
        if not metadata or not engine:
            return
        
        schema = metadata.get("local_schema")
        stats = metadata.get("stats")
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

                if cardinality > cardinality_threshold or row_count < 50:
                    continue

                limit = min(500, row_count)

                with engine.connect() as connection:
                    try:
                        result = connection.execute(text(
                            f"SELECT DISTINCT {col_name} FROM {table} WHERE {col_name} IS NOT NULL LIMIT {limit}"
                        ))
                        for row in result:
                            value = str(row[0])
                            self.add_value(connection_string, table, col_name, value)
                    except Exception as e:
                        print(f"[embedding] Failed {table}.{col_name}: {e}")

    def _is_text_type(self, col_type: str) -> bool:
        return any(t in col_type for t in ["text", "char", "varchar", "string"])

    def printCache(self):
        print(self.cache)
