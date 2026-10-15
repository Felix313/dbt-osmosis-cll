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


UsageKind = Literal[
    "join_key",
    "join_filter",
    "filter",
    "qualify",
    "window",
    "group_by",
    "having",
    "compile_time",
]
"""How a column is used *besides* being projected:

- ``join_key``     column of a JOIN ... ON predicate that relates two relations
                   (``a.k = b.k``, range joins, ``x BETWEEN b.from AND b.to``)
- ``join_filter``  JOIN ... ON predicate that only constrains one side (``AND b.flg = 'Y'``)
- ``filter``       WHERE predicate, including the projections of a filtering subquery
- ``qualify``      QUALIFY clause, window specification included
- ``window``       PARTITION BY / ORDER BY of a window function in a projection
- ``group_by``     GROUP BY key (``GROUP BY ALL`` expands to the non-aggregate projections)
- ``having``       HAVING predicate
- ``compile_time`` read by a Jinja query at compile time (e.g. ``get_column_values``), so it
                   decides which SQL is generated without appearing in the compiled SQL
"""

UsageResolution = Literal[
    "qualified",
    "single_table",
    "unqualified_resolved",
    "select_alias",
    "ambiguous",
    "unresolved",
    "no_upstream",
    "jinja",
]
"""How the column reference was mapped to its source relation:

- ``qualified``            ``alias.column`` — alias resolved within the scope
- ``single_table``         unqualified, the scope reads exactly one relation
- ``unqualified_resolved`` unqualified, exactly one in-scope relation has the column
- ``select_alias``         unqualified name of a projection of the same SELECT
- ``ambiguous``            unqualified, several (or no known) in-scope relations qualify;
                           one row is emitted per candidate
- ``unresolved``           qualifier or relation could not be mapped
- ``no_upstream``          the column is computed inside the model from no upstream column
                           (literal, ``ROW_NUMBER()``); its inputs appear as separate rows
- ``jinja``                taken from a compile-time Jinja call in the raw model code
"""


class ColumnUsage(BaseModel):
    """One non-projection use of an upstream column by a model's SQL.

    Kept separate from :class:`ColumnLineage` on purpose: whether a column that only
    filters or joins is still needed after a migration is a business decision, so the
    usage kind travels with every row instead of merging into the projected lineage.
    """

    usage: UsageKind
    source: str
    """``relation.column`` the reference resolves to through all CTE hops; ``""`` when
    the column has no upstream (see ``resolution``)."""
    column_ref: str
    """The reference as written in the SQL, e.g. ``rdv.dw_from_timestamp``."""
    scope: str
    """CTE name the clause sits in, ``<final>`` for the outermost SELECT, ``/subquery``
    appended for nested SELECTs, ``<jinja>`` for compile-time calls."""
    detail: Optional[str] = None
    """Position within the clause: ``predicate``, ``partition_by``, ``order_by``,
    ``window_function``, ``key``, ``subquery``, ``value_column``, ``where``."""
    predicate_sql: Optional[str] = None
    """The conjunct / window / Jinja call the column appears in, normalized SQL."""
    resolution: UsageResolution = "qualified"
    dbt_vars: List[str] = Field(default_factory=list)
    """Jinja variables the row is tied to (compile-time result variables, ``var()``)."""
    controls_branch: bool = False
    """True when the compile-time result decides an ``{% if %}`` branch of the model."""

    def sort_key(self) -> tuple:
        return (
            self.usage,
            self.source,
            self.scope,
            self.detail or "",
            self.column_ref,
            self.predicate_sql or "",
            self.resolution,
        )


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
    alias: Optional[str] = None
    """dbt ``alias`` — the relation name compiled SQL actually references, when it
    differs from ``name``. Set for models with a custom alias and for versioned
    models (``stg_customers`` vs ``stg_customers_v2``), which all share one ``name``."""
    source_identifier: Optional[str] = None
    source_name: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    column_usages: List[ColumnUsage] = Field(default_factory=list)
    """Join / filter / qualify / window / group-by / compile-time uses of upstream
    columns, filled by the SQL parse (sorted, de-duplicated)."""


class SQLParseResult(BaseModel):
    column_lineage: Dict[str, List[ColumnLineage]]
    column_usages: List[ColumnUsage] = Field(default_factory=list)
    """Non-projection column uses found in the SQL (see :class:`ColumnUsage`)."""
    star_sources: Set[str] = Field(default_factory=set)
    ephemeral_cte_lineage: Dict[str, Dict[str, ColumnLineage]] = Field(default_factory=dict)
    """Populated only when stop_at_ephemeral=True.
    Maps lowercased __dbt__cte__ name → {col_name: ColumnLineage} for each
    ephemeral CTE found in the compiled SQL."""
