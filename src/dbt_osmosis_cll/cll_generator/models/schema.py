from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Set, Dict, Literal, Any


class ColumnLineage(BaseModel):
    source_columns: Set[str]
    transformation_type: Literal[
        "direct", "renamed", "derived", "aggregate", "window", "union", "literal", "generated"
    ]
    sql_expression: Optional[str] = None
    description: Optional[str] = None
    union_branches: List[str] = Field(default_factory=list)
    """When transformation_type=="union", contains one qualified ``table.column``
    string per UNION/INTERSECT/EXCEPT branch in declaration order. Empty when
    the column is not produced by a top-level set operation. Used by downstream
    consumers (e.g. dbt-osmosis description inheritance) to look up each
    branch's upstream description and apply agreement-based dedup."""

    @property
    def is_rename(self) -> bool:
        """True when this lineage entry represents a pure column rename (aliased bare ColumnRef)."""
        return self.transformation_type == "renamed"

    @property
    def source_column(self) -> Optional[str]:
        """The original column name before the rename, or None if not a rename."""
        if not self.is_rename or not self.source_columns:
            return None
        src = next(iter(sorted(self.source_columns)))
        return src.split(".")[-1].lower()


class Column(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    name: str
    model_name: str
    description: Optional[str] = None
    data_type: Optional[str] = None
    lineage: Optional[List[ColumnLineage]] = Field(default_factory=list)  # type: ignore
    metadata: Optional[Dict[str, Any]] = None

    @property
    def full_name(self) -> str:
        return f"{self.model_name}.{self.name}"


class Exposure(BaseModel):
    name: str
    type: str
    url: Optional[str] = None
    description: Optional[str] = None
    owner: Optional[Dict[str, Any]] = None
    unique_id: str
    depends_on_models: Set[str] = Field(default_factory=set)
    resource_path: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class ModelDependency(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    model_name: str
    depends_on: Set[str]


class Model(BaseModel):
    model_config = ConfigDict(populate_by_name=True, protected_namespaces=())

    name: str
    schema_name: str = Field(alias="schema")  # Handle base model shadow attribute `schema`
    database: str
    columns: Dict[str, Column] = Field(default_factory=dict)
    metadata: Optional[Dict[str, Any]] = None
    unique_id: Optional[str] = None
    upstream: Set[str] = Field(default_factory=set)
    downstream: Set[str] = Field(default_factory=set)
    compiled_sql: Optional[str] = None
    language: Optional[str] = None
    resource_type: Literal["model", "source", "seed", "test", "exposure", "snapshot"]
    resource_path: Optional[str] = None
    source_identifier: Optional[str] = None
    source_name: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class SQLParseResult(BaseModel):
    column_lineage: Dict[str, List[ColumnLineage]]
    star_sources: Set[str] = Field(default_factory=set)
    ephemeral_cte_lineage: Dict[str, Dict[str, ColumnLineage]] = Field(default_factory=dict)
    """Populated only when stop_at_ephemeral=True.
    Maps lowercased __dbt__cte__ name → {col_name: ColumnLineage} for each
    ephemeral CTE found in the compiled SQL."""
