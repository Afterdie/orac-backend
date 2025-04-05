from pydantic import BaseModel
from typing import List, Dict

class ColumnSchema(BaseModel):
    name: str
    type: str
    nullable: bool

class ForeignKeySchema(BaseModel):
    column: List[str]
    references_table: str
    referenced_column: List[str]

class RelationshipSchema(BaseModel):
    from_table: str
    from_columns: List[str]
    to_table: str
    to_columns: List[str]

class IndexSchema(BaseModel):
    name: str
    columns: List[str]
    unique: bool

class TableSchema(BaseModel):
    columns: List[ColumnSchema]
    foreign_keys: List[ForeignKeySchema]
    relationships: List[RelationshipSchema]
    indexes: List[IndexSchema]

class TableStats(BaseModel):
    row_count: int
    cardinality: Dict[str, float]

class Metadata(BaseModel):
    local_schema: Dict[str, TableSchema]
    stats: Dict[str, TableStats]