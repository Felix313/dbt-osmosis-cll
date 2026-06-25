"""
Minimal YAML renderer for dbt model YAML files.

Produces stable, human-readable output without relying on ruamel.yaml's
round-trip emitter (which can reorder keys or alter block scalars).
Only handles the subset of the dbt YAML schema used by staging/model files:
  version: 2 / models: [...columns: [...]]
"""

from __future__ import annotations

import re
import textwrap


_DEFAULT_MAX_LINE_WIDTH = 150


def _quote(s: str) -> str:
    escaped = s.replace("'", "''")
    return f"'{escaped}'"


def _needs_quoting(s: str) -> bool:
    return bool(re.search(r"[:{}\[\]#&*!|>'\"%@`]", s)) or bool(s and s[0] in "-?")


def _render_scalar(s: str) -> str:
    return _quote(s) if _needs_quoting(s) else s


def _render_value(value: object, indent: int) -> list[str]:
    pad = " " * indent
    if isinstance(value, dict):
        lines: list[str] = []
        for k, v in value.items():
            sub = _render_value(v, indent + 2)
            if len(sub) == 1 and not sub[0].startswith(" " * (indent + 2)):
                lines.append(f"{pad}{k}: {sub[0].strip()}")
            else:
                lines.append(f"{pad}{k}:")
                lines.extend(sub)
        return lines
    if isinstance(value, list):
        lines = []
        for item in value:
            if isinstance(item, dict):
                sub_items = list(item.items())
                first_k, first_v = sub_items[0]
                sub = _render_value(first_v, indent + 4)
                if len(sub) == 1 and not isinstance(first_v, (dict, list)):
                    lines.append(f"{pad}- {first_k}: {sub[0].strip()}")
                else:
                    lines.append(f"{pad}- {first_k}:")
                    lines.extend(sub)
                for k, v in sub_items[1:]:
                    sub = _render_value(v, indent + 4)
                    if len(sub) == 1 and not isinstance(v, (dict, list)):
                        lines.append(f"{pad}  {k}: {sub[0].strip()}")
                    else:
                        lines.append(f"{pad}  {k}:")
                        lines.extend(sub)
            else:
                lines.append(f"{pad}- {_render_scalar(str(item))}")
        return lines
    if value is None:
        return [""]
    return [_render_scalar(str(value))]


def _wrap_description(desc: str, max_text_width: int) -> str:
    """Word-wrap each paragraph, preserving blank-line separators."""
    paragraphs = desc.split("\n\n")
    wrapped = []
    for para in paragraphs:
        rewrapped: list[str] = []
        for line in para.splitlines():
            if len(line) > max_text_width:
                rewrapped.extend(textwrap.wrap(line, max_text_width))
            else:
                rewrapped.append(line)
        wrapped.append("\n".join(rewrapped))
    return "\n\n".join(wrapped)


def _render_description(
    desc: str, key_indent: int, max_line_width: int = _DEFAULT_MAX_LINE_WIDTH
) -> list[str]:
    """Render a description as a literal block scalar when multi-line or too long."""
    max_text_width = max_line_width - (key_indent + 2)
    if "\n" not in desc and len(desc) > max_text_width:
        desc = _wrap_description(desc, max_text_width)
    if "\n" in desc:
        pad = " " * (key_indent + 2)
        lines = [f"{'  ' * (key_indent // 2)}description: |"]
        for line in desc.splitlines():
            lines.append(f"{pad}{line}" if line.strip() else "")
        return lines
    return [f"{'  ' * (key_indent // 2)}description: {_render_scalar(desc)}"]


def render_model_yml(data: dict, max_line_width: int = _DEFAULT_MAX_LINE_WIDTH) -> str:
    """Render a dbt model YAML dict to a stable string (``version: 2 / models:`` schema)."""
    lines = ["version: 2", "models:"]
    for model in data.get("models", []):
        lines.append(f"  - name: {model['name']}")
        if desc := model.get("description"):
            lines += _render_description(desc, key_indent=4, max_line_width=max_line_width)
        for k, v in model.items():
            if k in ("name", "description", "columns"):
                continue
            sub = _render_value(v, indent=6)
            if len(sub) == 1 and not isinstance(v, (dict, list)):
                lines.append(f"    {k}: {sub[0].strip()}")
            else:
                lines.append(f"    {k}:")
                lines.extend(sub)
        if cols := model.get("columns"):
            lines.append("    columns:")
            for col in cols:
                lines.append(f"      - name: {col['name']}")
                if desc := col.get("description"):
                    lines += _render_description(desc, key_indent=8, max_line_width=max_line_width)
                for k, v in col.items():
                    if k in ("name", "description"):
                        continue
                    sub = _render_value(v, indent=10)
                    if len(sub) == 1 and not isinstance(v, (dict, list)):
                        lines.append(f"        {k}: {sub[0].strip()}")
                    else:
                        lines.append(f"        {k}:")
                        lines.extend(sub)
    return "\n".join(lines) + "\n"
