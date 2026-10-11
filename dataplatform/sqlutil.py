"""Small helpers for building SQL safely.

Every module here builds SQL by string composition, because Spark SQL has no
bind parameters for DDL/MERGE. Partner filenames, source names and check
rationales all reach these statements, and at least one of them will eventually
contain an apostrophe. These two functions are the only sanctioned way to put a
runtime value into a statement.
"""

from __future__ import annotations

import re

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")


def sql_str(value: object) -> str:
    """Render a value as a single-quoted SQL string literal.

    Doubles embedded quotes and rejects the characters that would let a value
    escape the literal entirely. Returns the literal NULL for None so callers
    do not have to special-case it.
    """
    if value is None:
        return "NULL"
    text = str(value)
    if "\x00" in text:
        raise ValueError("NUL byte in SQL literal")
    return "'" + text.replace("\\", "\\\\").replace("'", "''") + "'"


def sql_ident(name: str) -> str:
    """Validate a table/column identifier used in a position that cannot be a literal.

    Identifiers come from configuration, not from data, so the right behaviour
    is to fail loudly on anything unexpected rather than to try to escape it.
    """
    if not _IDENT.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


def catalog_of(table: str) -> str:
    """The catalog half of a fully-qualified `catalog.namespace.table`.

    Procedures and DDL are catalog-scoped, so this is what stops the modules
    from hardcoding a catalog name and only working in one environment.
    """
    parts = sql_ident(table).split(".")
    if len(parts) < 3:
        raise ValueError(f"expected catalog.namespace.table, got {table!r}")
    return parts[0]


def relative_to_catalog(table: str) -> str:
    """`catalog.ns.tbl` -> `ns.tbl`, the form Iceberg CALL procedures expect."""
    return sql_ident(table).split(".", 1)[1]

