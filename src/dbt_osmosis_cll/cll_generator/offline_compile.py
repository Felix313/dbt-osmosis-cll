"""Offline ``dbt compile`` for column-level lineage (``dbt-osmosis-cll compile-cll``).

Column lineage needs the SQL a model *really* runs. Many projects decide that SQL at
compile time by asking the warehouse:

- ``get_column_values(ref('load_config'), 'load_flg')`` picks the real body or a dummy,
- ``is_incremental()`` checks whether the target table exists,
- ``star(from=ref('x'))`` lists the relation's columns.

Without a connection those calls fail or return defaults, and ``target/compiled/``
holds the wrong branch. This module compiles without any warehouse connection by
parsing the project in-process and replacing such macros *in the parsed manifest*
with stubs driven by the ``[compile]`` section of ``.osmosis-cll``. Nothing in the
dbt project is modified. Every assumption a stub makes is recorded in
``target/cll-compile.json`` and logged, and compiled models that look like an empty
shell (``LIMIT 0``, ``WHERE 1 = 0``, reading only their own relation) are flagged.

``[compile]`` keys (all optional)::

    [compile]
    adapter = snowflake            # adapter of the generated offline profile
    target = cll_offline           # target name the models see (target.name)
    database = ANALYTICS           # database / schema of the generated profile
    schema = PUBLIC
    profiles-dir = ~/.dbt          # use an existing profile instead (never connected)
    is-incremental = false         # what every is_incremental() returns
    is-incremental-models = m1, m2 # models for which is_incremental() returns true
    introspection-values =         # pinned results of get_column_values-style calls
        load_config.load_flg = Y
        load_config.run_dt = 2000-01-31
    vars = {"run_ts": "2000-01-01T00:00:00"}
    macro-stubs =                  # any other introspective macro: name = Jinja expression
        my_pkg.get_max_loaded_ts = '2000-01-01'
    relations-exist = audit_log    # adapter.get_relation(): treat as existing ...
    relations-missing = tmp_table  # ... or as missing (default: exists iff in the manifest)

Direct adapter calls in model Jinja are answered offline as well:
``adapter.get_relation()`` returns a relation for everything the manifest knows (or
``relations-exist``) and None otherwise; ``adapter.get_columns_in_relation()`` returns the
manifest's documented columns. Each answer is recorded per model.
"""

from __future__ import annotations

import configparser
import json
import logging
import re
import tempfile
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dbt_osmosis_cll.config import find_config_file, get_config

logger = logging.getLogger(__name__)

SIDECAR_NAME = "cll-compile.json"
_SECTION = "compile"
_LOG_MARKER = "[cll-offline] "
_STATE_VAR = "__cll_offline"


class OfflineCompileError(RuntimeError):
    """Raised when the offline compile cannot run or a model still needs the warehouse."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class CompileConfig:
    adapter: str | None = None
    target: str = "cll_offline"
    database: str | None = None
    schema: str | None = None
    profiles_dir: str | None = None
    is_incremental: bool = False
    incremental_models: list[str] = field(default_factory=list)
    introspection_values: dict[str, list[str]] = field(default_factory=dict)
    vars: dict[str, Any] = field(default_factory=dict)
    macro_stubs: dict[str, str] = field(default_factory=dict)
    value_macros: list[str] = field(default_factory=lambda: ["get_column_values"])
    """Macro names with the ``get_column_values(table, column, ...)`` signature."""
    relations_exist: list[str] = field(default_factory=list)
    relations_missing: list[str] = field(default_factory=list)


def _pairs(raw: str | None) -> list[tuple[str, str]]:
    """``key = value`` lines of a multi-line INI value."""
    out = []
    for line in (raw or "").splitlines():
        line = line.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if "=" not in line:
            raise OfflineCompileError(f"[compile]: expected 'key = value', got {line!r}")
        key, value = line.split("=", 1)
        out.append((key.strip(), value.strip()))
    return out


def load_compile_config(project_dir: Path) -> CompileConfig:
    """Read the ``[compile]`` section of the project's ``.osmosis-cll`` (defaults if absent)."""
    cfg = CompileConfig(value_macros=list(get_config(project_dir).compile_time_query_macros))
    path = find_config_file(project_dir)
    if path is None:
        return cfg
    # No interpolation: values are SQL / Jinja and may contain '%'.
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(path, encoding="utf-8")
    if not parser.has_section(_SECTION):
        return cfg
    sec = parser[_SECTION]
    known = {
        "adapter",
        "target",
        "database",
        "schema",
        "profiles-dir",
        "is-incremental",
        "is-incremental-models",
        "introspection-values",
        "vars",
        "macro-stubs",
        "relations-exist",
        "relations-missing",
    }
    unknown = set(sec) - known
    if unknown:
        logger.warning(
            "Unknown settings in %s [compile]: %s — ignored.", path, ", ".join(sorted(unknown))
        )
    cfg.adapter = sec.get("adapter") or None
    cfg.target = sec.get("target", cfg.target).strip() or cfg.target
    cfg.database = sec.get("database") or None
    cfg.schema = sec.get("schema") or None
    if sec.get("profiles-dir"):
        cfg.profiles_dir = str(Path(sec["profiles-dir"]).expanduser())
    cfg.is_incremental = sec.getboolean("is-incremental", False)
    cfg.incremental_models = sorted(
        m.strip().lower() for m in sec.get("is-incremental-models", "").split(",") if m.strip()
    )
    for key, value in _pairs(sec.get("introspection-values")):
        cfg.introspection_values[key.lower()] = [v.strip() for v in value.split(",")]
    if sec.get("vars"):
        try:
            cfg.vars = json.loads(sec["vars"])
        except json.JSONDecodeError as exc:
            raise OfflineCompileError(f"[compile] vars must be JSON: {exc}") from exc
    cfg.macro_stubs = dict(_pairs(sec.get("macro-stubs")))
    for key, attr in (
        ("relations-exist", "relations_exist"),
        ("relations-missing", "relations_missing"),
    ):
        setattr(
            cfg,
            attr,
            sorted(
                r.strip().lower()
                for r in sec.get(key, "").replace("\n", ",").split(",")
                if r.strip()
            ),
        )
    return cfg


# ---------------------------------------------------------------------------
# Offline profile
# ---------------------------------------------------------------------------

_DUMMY_CREDENTIALS: dict[str, dict[str, Any]] = {
    "snowflake": {
        "account": "offline",
        "user": "offline",
        "password": "offline",
        "warehouse": "OFFLINE",
    },
    "duckdb": {"path": ":memory:"},
    "postgres": {
        "host": "localhost",
        "user": "offline",
        "password": "offline",
        "port": 5432,
        "dbname": "offline",
    },
    "redshift": {
        "host": "localhost",
        "user": "offline",
        "password": "offline",
        "port": 5439,
        "dbname": "offline",
    },
    "bigquery": {"method": "oauth", "project": "offline", "dataset": "offline"},
    "databricks": {"host": "offline", "http_path": "/offline", "token": "offline"},
}


def _project_profile_name(project_dir: Path) -> str:
    from ruamel.yaml import YAML

    data = YAML(typ="safe").load((project_dir / "dbt_project.yml").read_text(encoding="utf-8"))
    name = (data or {}).get("profile")
    if not name:
        raise OfflineCompileError(
            "dbt_project.yml has no 'profile' — cannot build an offline profile"
        )
    return str(name)


def _infer_adapter(project_dir: Path) -> str | None:
    manifest = project_dir / "target" / "manifest.json"
    if manifest.exists():
        try:
            head = manifest.read_text(encoding="utf-8")[:4000]
            match = re.search(r'"adapter_type"\s*:\s*"([^"]+)"', head)
            if match:
                return match.group(1)
        except OSError:
            pass
    return None


def write_offline_profile(project_dir: Path, cfg: CompileConfig, out_dir: Path) -> Path:
    """Write a profiles.yml whose credentials can never reach a warehouse."""
    adapter = cfg.adapter or _infer_adapter(project_dir)
    if not adapter:
        raise OfflineCompileError(
            "Set 'adapter' in the [compile] section of .osmosis-cll (or 'profiles-dir') — "
            "the offline profile needs the adapter type."
        )
    output: dict[str, Any] = {"type": adapter, "threads": 1}
    output.update(_DUMMY_CREDENTIALS.get(adapter, {}))
    if adapter == "bigquery":
        output["project"] = cfg.database or output["project"]
        output["dataset"] = cfg.schema or output["dataset"]
    else:
        output["database"] = cfg.database or "OFFLINE"
        output["schema"] = cfg.schema or "OFFLINE"
    if adapter in ("postgres", "redshift"):
        output.pop("database")
        output["dbname"] = cfg.database or output["dbname"]
    if adapter == "duckdb":
        output.pop("database")
    profile = {
        _project_profile_name(project_dir): {"target": cfg.target, "outputs": {cfg.target: output}}
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "profiles.yml"
    path.write_text(json.dumps(profile, indent=2), encoding="utf-8")  # JSON is valid YAML
    return out_dir


# ---------------------------------------------------------------------------
# Macro stubs
# ---------------------------------------------------------------------------

_MACRO_HEADER = re.compile(r"\{%-?\s*macro\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(([^)]*)\)")


def _macro_params(macro_sql: str, name: str) -> list[str] | None:
    for match in _MACRO_HEADER.finditer(macro_sql):
        if match.group(1) == name:
            return [p.split("=")[0].strip() for p in match.group(2).split(",") if p.strip()]
    return None


def _log_call(payload: str) -> str:
    return f"{{{{ log({_LOG_MARKER!r} ~ tojson({payload}), info=false) }}}}"


_CURRENT_MODEL = (
    "((model.name if model is defined and model and model.name is defined else '') | lower)"
)


def _stub_is_incremental() -> str:
    return (
        "{% macro is_incremental() %}"
        f"{{%- set cfg = var('{_STATE_VAR}', {{}}) -%}}"
        f"{{%- set name = {_CURRENT_MODEL} -%}}"
        "{%- set value = (name in cfg.get('incremental_models', [])) or cfg.get('is_incremental', false) -%}"
        + _log_call("{'kind': 'is_incremental', 'model': name, 'value': value}")
        + "{{ return(value) }}{% endmacro %}"
    )


def _stub_value_macro(name: str) -> str:
    rel = (
        "((table.identifier if table.identifier is defined and table.identifier "
        "else (table | string).split('.')[-1]) | replace('\"', '') | lower)"
    )
    return (
        f"{{% macro {name}(table, column, order_by='count(*) desc', max_records=none, "
        "default=none, where=none) -%}"
        f"{{%- set cfg = var('{_STATE_VAR}', {{}}) -%}}"
        f"{{%- set key = {rel} ~ '.' ~ (column | replace('\"', '') | lower) -%}}"
        "{%- set values = cfg.get('values', {}) -%}"
        "{%- if key in values -%}"
        + _log_call(f"{{'kind': 'value', 'model': {_CURRENT_MODEL}, 'key': key, 'pinned': true}}")
        + "{{ return(values[key]) }}{%- endif -%}"
        + _log_call(f"{{'kind': 'value', 'model': {_CURRENT_MODEL}, 'key': key, 'pinned': false}}")
        + "{{ return(default if default is not none else []) }}{%- endmacro %}"
    )


def _stub_star(name: str) -> str:
    """Offline star: the documented columns from the graph, else ``*`` (with EXCLUDE)."""
    return (
        f"{{% macro {name}(from, relation_alias=False, except=[], prefix='', suffix='', "
        "quote_identifiers=True) -%}"
        "{%- set _extra = varargs -%}{%- set rename = kwargs.get('rename', {}) -%}"
        f"{{%- set cfg = var('{_STATE_VAR}', {{}}) -%}}"
        "{%- set ident = ((from.identifier if from.identifier is defined and from.identifier "
        "else (from | string).split('.')[-1]) | replace('\"', '') | lower) -%}"
        "{%- set excluded = except | map('lower') | list -%}"
        "{%- set cols = [] -%}"
        "{%- for node in graph.nodes.values() if node.resource_type in ['model', 'seed', 'snapshot'] "
        "and ((node.alias or node.name) | lower) == ident -%}"
        "{%- for col in node.columns.keys() if (col | lower) not in excluded and col not in cols -%}"
        "{%- do cols.append(col) -%}{%- endfor -%}{%- endfor -%}"
        "{%- for node in graph.sources.values() if ((node.identifier or node.name) | lower) == ident -%}"
        "{%- for col in node.columns.keys() if (col | lower) not in excluded and col not in cols -%}"
        "{%- do cols.append(col) -%}{%- endfor -%}{%- endfor -%}"
        "{%- set q = (relation_alias ~ '.') if relation_alias else '' -%}"
        + _log_call(
            f"{{'kind': 'star', 'model': {_CURRENT_MODEL}, 'relation': ident, "
            "'known_columns': (cols | length) > 0}"
        )
        + "{%- if cols -%}{%- set out = [] -%}{%- for col in cols -%}"
        "{%- if prefix or suffix or rename -%}"
        "{%- do out.append(q ~ col ~ ' AS ' ~ prefix ~ rename.get(col, col) ~ suffix) -%}"
        "{%- else -%}{%- do out.append(q ~ col) -%}{%- endif -%}{%- endfor -%}"
        "{{ return(out | join(', ')) }}{%- endif -%}"
        "{%- set kw = {'snowflake': 'EXCLUDE', 'duckdb': 'EXCLUDE', 'bigquery': 'EXCEPT', "
        "'databricks': 'EXCEPT', 'spark': 'EXCEPT'}.get(cfg.get('adapter', ''), '') -%}"
        "{%- if except and kw -%}{{ return(q ~ '* ' ~ kw ~ ' (' ~ (except | join(', ')) ~ ')') }}{%- endif -%}"
        "{{ return(q ~ '*') }}{%- endmacro %}"
    )


def _stub_custom(name: str, expression: str) -> str:
    return (
        f"{{% macro {name}() %}}{{%- set _a = varargs -%}}{{%- set _k = kwargs -%}}"
        + _log_call(f"{{'kind': 'macro_stub', 'model': {_CURRENT_MODEL}, 'macro': {name!r}}}")
        + f"{{{{ return({expression}) }}}}{{% endmacro %}}"
    )


def stub_introspective_macros(manifest: Any, cfg: CompileConfig, adapter: str) -> list[str]:
    """Replace introspective macros in a parsed manifest; return the stubbed unique_ids.

    Matching is by macro name in *any* package (dbt's own, dbt_utils, project or
    in-house packages). Signature-checked replacements (``table, column`` /
    ``from``) leave same-named macros with a different contract untouched.
    """
    value_names = {n for n in cfg.value_macros}
    stubbed: list[str] = []
    custom: dict[tuple[str | None, str], str] = {}
    for key, expr in cfg.macro_stubs.items():
        pkg, _, name = key.rpartition(".")
        custom[(pkg or None, name)] = expr
    dispatch_prefixes = ("default__", f"{adapter}__")

    for uid, macro in sorted(manifest.macros.items()):
        name = macro.name
        base = name
        for prefix in dispatch_prefixes:
            if name.startswith(prefix):
                base = name[len(prefix) :]
        params = _macro_params(macro.macro_sql, name) or []
        new_sql = None
        if (macro.package_name, name) in custom or (None, name) in custom:
            new_sql = _stub_custom(
                name, custom.get((macro.package_name, name), custom.get((None, name), "none"))
            )
        elif name == "is_incremental" and not params:
            new_sql = _stub_is_incremental()
        elif base in value_names and params[:2] == ["table", "column"]:
            new_sql = _stub_value_macro(name)
        elif base == "star" and params[:1] == ["from"]:
            new_sql = _stub_star(name)
        if new_sql is not None:
            macro.macro_sql = new_sql
            stubbed.append(uid)
    return stubbed


# ---------------------------------------------------------------------------
# Empty-shell detection
# ---------------------------------------------------------------------------


def detect_empty_shell(sql: str, relation: str, dialect: str | None) -> list[str]:
    """Signals that a compiled model is a placeholder rather than its real logic."""
    from sqlglot import exp, parse_one

    try:
        tree = parse_one(sql, dialect=dialect)
    except Exception:  # noqa: BLE001 — unparseable SQL is reported by parse-cll itself
        return []
    flags: list[str] = []
    final = tree
    while isinstance(final, (exp.Subquery,)):
        final = final.this
    limit = final.args.get("limit") if isinstance(final, exp.Query) else None
    if limit is not None:
        value = limit.expression if hasattr(limit, "expression") else None
        if isinstance(value, exp.Literal) and str(value.this) == "0":
            flags.append("limit_0")
    where = final.args.get("where") if isinstance(final, exp.Select) else None
    if where is not None:
        cond = where.this
        if isinstance(cond, exp.Boolean) and not cond.this:
            flags.append("false_predicate")
        elif isinstance(cond, (exp.EQ, exp.NEQ)) and all(
            isinstance(side, exp.Literal) for side in (cond.this, cond.expression)
        ):
            same = str(cond.this.this) == str(cond.expression.this)
            if same != isinstance(cond, exp.EQ):
                flags.append("false_predicate")
    cte_names = {c.alias.lower() for c in tree.find_all(exp.CTE)}
    tables = {t.name.lower() for t in tree.find_all(exp.Table) if t.name.lower() not in cte_names}
    if tables and tables == {relation.lower()}:
        flags.append("self_only")
    return flags


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------


@dataclass
class OfflineCompileReport:
    compiled_models: list[str] = field(default_factory=list)
    stubbed_macros: list[str] = field(default_factory=list)
    models: dict[str, dict[str, Any]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def warnings(self) -> list[str]:
        out = []
        for model, info in sorted(self.models.items()):
            if info.get("is_incremental") is not None:
                out.append(
                    f"{model}: compiled with is_incremental() = {info['is_incremental']} — assumed "
                    "to produce the model's complete logic (verify if the branch builds a stub)"
                )
            for key in info.get("unpinned_values", []):
                out.append(f"{model}: no pinned value for {key} — the macro returned its default")
            for rel in info.get("star_without_columns", []):
                out.append(f"{model}: star over {rel} without known columns — rendered as '*'")
            for flag in info.get("empty_shell", []):
                out.append(f"{model}: compiled SQL looks like an empty shell ({flag})")
            for rel in info.get("relations_assumed_missing", []):
                out.append(f"{model}: adapter.get_relation({rel}) answered 'does not exist'")
        return out

    def to_json(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "compiled_models": sorted(self.compiled_models),
            "stubbed_macros": sorted(self.stubbed_macros),
            "models": {k: self.models[k] for k in sorted(self.models)},
            "warnings": self.warnings(),
            "errors": self.errors,
        }


def _manifest_relations(manifest: Any) -> dict[str, list[tuple[str, str | None]]]:
    """relation identifier (lowercase) → documented ``(column, data_type)`` pairs."""
    out: dict[str, list[tuple[str, str | None]]] = {}
    for node in list(manifest.nodes.values()) + list(manifest.sources.values()):
        if getattr(node, "resource_type", None) not in ("model", "seed", "snapshot", "source"):
            continue
        ident = (
            getattr(node, "identifier", None) or getattr(node, "alias", None) or node.name
        ).lower()
        cols = [(c.name, getattr(c, "data_type", None)) for c in (node.columns or {}).values()]
        out.setdefault(ident, [])
        out[ident].extend(c for c in cols if c not in out[ident])
    return out


def _current_model() -> str:
    try:
        from dbt_common.events.contextvars import get_node_info

        info = get_node_info() or {}
        return str(info.get("node_name") or "").lower()
    except Exception:  # noqa: BLE001
        return ""


@contextmanager
def _offline_adapter(
    adapter: str, manifest: Any, cfg: CompileConfig, events: list[dict[str, Any]]
) -> Generator[None, None, None]:
    """Answer the adapter's introspection offline and refuse real connections.

    ``get_relation`` / ``get_columns_in_relation`` are answered from the manifest
    (overridable via ``relations-exist`` / ``relations-missing``); opening a
    connection raises with a hint which setting to add.
    """
    try:
        from dbt.adapters.contracts.relation import RelationType
        from dbt.adapters.factory import FACTORY

        adapter_cls = FACTORY.get_adapter_class_by_name(adapter)
    except Exception:  # noqa: BLE001 — unknown adapter: dbt's own errors will surface
        yield
        return
    manager = adapter_cls.ConnectionManager
    known = _manifest_relations(manifest)
    exist = set(cfg.relations_exist)
    missing = set(cfg.relations_missing)
    saved = {
        "open": manager.__dict__.get("open"),
        "get_relation": adapter_cls.__dict__.get("get_relation"),
        "get_columns_in_relation": adapter_cls.__dict__.get("get_columns_in_relation"),
    }

    def _refuse(cls, connection):
        raise OfflineCompileError(
            "A model or macro tried to open a warehouse connection during the offline compile "
            f"({_current_model() or 'unknown model'}). Pin the result via [compile] "
            "introspection-values / macro-stubs in .osmosis-cll."
        )

    def _get_relation(self, database, schema, identifier):
        ident = str(identifier or "").strip('"').lower()
        if ident in missing:
            exists, reason = False, "relations-missing"
        elif ident in exist:
            exists, reason = True, "relations-exist"
        else:
            exists, reason = ident in known, "manifest"
        events.append({
            "kind": "get_relation",
            "model": _current_model(),
            "relation": ident,
            "exists": exists,
            "reason": reason,
        })
        if not exists:
            return None
        return self.Relation.create(
            database=database, schema=schema, identifier=identifier, type=RelationType.Table
        )

    def _get_columns(self, relation):
        ident = str(getattr(relation, "identifier", "") or "").strip('"').lower()
        cols = known.get(ident, [])
        events.append({
            "kind": "columns",
            "model": _current_model(),
            "relation": ident,
            "known_columns": bool(cols),
        })
        return [self.Column.create(name, dtype or "TEXT") for name, dtype in cols]

    manager.open = classmethod(_refuse)
    adapter_cls.get_relation = _get_relation
    adapter_cls.get_columns_in_relation = _get_columns
    try:
        yield
    finally:
        for owner, name in (
            (manager, "open"),
            (adapter_cls, "get_relation"),
            (adapter_cls, "get_columns_in_relation"),
        ):
            if saved[name] is not None:
                setattr(owner, name, saved[name])
            else:
                delattr(owner, name)


def run_offline_compile(
    project_dir: Path,
    select: list[str],
    exclude: list[str] | None = None,
    cfg: CompileConfig | None = None,
    profiles_dir: Path | None = None,
    event_sink: Callable[[str], None] | None = None,
) -> OfflineCompileReport:
    """Parse, stub introspective macros, compile *select* offline; write the sidecar."""
    from dbt.cli.main import dbtRunner

    project_dir = Path(project_dir).resolve()
    cfg = cfg or load_compile_config(project_dir)
    adapter = cfg.adapter or _infer_adapter(project_dir) or ""
    tmp = None
    if profiles_dir is None and cfg.profiles_dir:
        profiles_dir = Path(cfg.profiles_dir)
    if profiles_dir is None:
        tmp = tempfile.TemporaryDirectory(prefix="cll-offline-profile-")
        profiles_dir = write_offline_profile(project_dir, cfg, Path(tmp.name))
        adapter = adapter or (cfg.adapter or "")

    state = {
        "values": cfg.introspection_values,
        "incremental_models": cfg.incremental_models,
        "is_incremental": cfg.is_incremental,
        "adapter": adapter,
    }
    run_vars = {**cfg.vars, _STATE_VAR: state}
    common = [
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--target",
        cfg.target,
        "--vars",
        json.dumps(run_vars, sort_keys=True),
    ]

    stub_events: list[dict[str, Any]] = []

    def _collect(event: Any) -> None:
        try:
            msg = event.info.msg if hasattr(event, "info") else ""
            if not msg:
                return
            if _LOG_MARKER in msg:
                stub_events.append(json.loads(msg.split(_LOG_MARKER, 1)[1]))
            elif event_sink is not None and event.info.level in ("info", "warn", "error"):
                event_sink(msg)
        except Exception:  # noqa: BLE001 — logging must never break the compile
            return

    report = OfflineCompileReport()
    try:
        parsed = dbtRunner(callbacks=[_collect]).invoke(["parse", *common])
        if not parsed.success or parsed.result is None:
            raise OfflineCompileError(f"dbt parse failed: {parsed.exception}")
        manifest = parsed.result
        report.stubbed_macros = stub_introspective_macros(manifest, cfg, adapter)

        # Tests are never needed for lineage and would require a connection.
        args = [
            "compile",
            *common,
            "--select",
            *select,
            "--exclude",
            "resource_type:test",
            *(exclude or []),
            "--no-populate-cache",
            "--no-introspect",
            "--no-write-json",
        ]
        with _offline_adapter(adapter, manifest, cfg, stub_events):
            compiled = dbtRunner(manifest=manifest, callbacks=[_collect]).invoke(args)
    finally:
        if tmp is not None:
            tmp.cleanup()

    results = getattr(compiled.result, "results", None) or []
    for res in results:
        node = getattr(res, "node", None)
        if node is None or getattr(node, "resource_type", None) != "model":
            continue
        name = node.name.lower()
        status = str(getattr(res, "status", ""))
        if "error" in status.lower():
            report.errors.append(f"{node.name}: {getattr(res, 'message', '')}")
            continue
        report.compiled_models.append(name)
        info = report.models.setdefault(name, {})
        sql = getattr(node, "compiled_code", None) or ""
        relation = (getattr(node, "alias", None) or node.name).lower()
        flags = detect_empty_shell(sql, relation, adapter or None)
        if flags:
            info["empty_shell"] = flags
    if not compiled.success and compiled.exception is not None:
        report.errors.append(str(compiled.exception))

    for ev in stub_events:
        model = (ev.get("model") or "").lower()
        if not model:
            continue
        info = report.models.setdefault(model, {})
        kind = ev.get("kind")
        if kind == "is_incremental":
            info["is_incremental"] = bool(ev.get("value"))
        elif kind == "value":
            bucket = "pinned_values" if ev.get("pinned") else "unpinned_values"
            info[bucket] = sorted(set(info.get(bucket, [])) | {ev.get("key", "")})
        elif kind == "star" and not ev.get("known_columns"):
            info["star_without_columns"] = sorted(
                set(info.get("star_without_columns", [])) | {ev.get("relation", "")}
            )
        elif kind == "macro_stub":
            info["stubbed_calls"] = sorted(
                set(info.get("stubbed_calls", [])) | {ev.get("macro", "")}
            )
        elif kind == "get_relation":
            bucket = (
                "relations_assumed_existing" if ev.get("exists") else "relations_assumed_missing"
            )
            info[bucket] = sorted(set(info.get(bucket, [])) | {ev.get("relation", "")})
        elif kind == "columns" and not ev.get("known_columns"):
            info["star_without_columns"] = sorted(
                set(info.get("star_without_columns", [])) | {ev.get("relation", "")}
            )

    target_dir = project_dir / "target"
    target_dir.mkdir(exist_ok=True)
    (target_dir / SIDECAR_NAME).write_text(
        json.dumps(report.to_json(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return report


def load_sidecar(target_dir: Path) -> dict[str, Any] | None:
    path = target_dir / SIDECAR_NAME
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
