"""Checks every SQL statement the model writes before it reaches Athena.

The model is asked to write read-only queries against one table. This module
does not trust that: it rejects anything that is not a single SELECT over the
allowed table, and caps the number of rows returned. The Athena workgroup adds
a second limit of its own, on bytes scanned.
"""

from __future__ import annotations

import re

ALLOWED_TABLES = {"nfip_claims", "flood_claims_lakehouse_curated.nfip_claims"}
MAX_ROWS = 200

FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|drop|create|alter|truncate|grant|revoke|"
    r"msck|unload|call|execute|prepare|deallocate|describe|show|use|vacuum|optimize)\b",
    re.IGNORECASE,
)
TABLE_REF = re.compile(r"\b(?:from|join)\s+([A-Za-z_][\w.\"]*)", re.IGNORECASE)
CTE_NAME = re.compile(r"(?:\bwith|,)\s*([A-Za-z_]\w*)\s+as\s*\(", re.IGNORECASE)


class UnsafeSQL(ValueError):
    """The statement is not a query this project allows."""


def _strip_literals_and_comments(sql: str) -> str:
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"'(?:[^']|'')*'", "''", sql)


def check(sql: str) -> str:
    """Return the statement, row-capped and safe to run, or raise UnsafeSQL."""
    statement = sql.strip().rstrip(";").strip()
    if not statement:
        raise UnsafeSQL("the statement is empty")

    bare = _strip_literals_and_comments(statement)
    if ";" in bare:
        raise UnsafeSQL("only one statement is allowed")
    if not re.match(r"^\s*(select|with)\b", bare, re.IGNORECASE):
        raise UnsafeSQL("only SELECT statements are allowed")
    found = FORBIDDEN.search(bare)
    if found:
        raise UnsafeSQL(f"the keyword '{found.group(1).lower()}' is not allowed")

    cte_names = {name.lower() for name in CTE_NAME.findall(bare)}
    # "extract(year from date_of_loss)" uses FROM without naming a table.
    without_extract = re.sub(r"\bextract\s*\(\s*\w+\s+from\b", "extract(", bare, flags=re.IGNORECASE)
    for reference in TABLE_REF.findall(without_extract):
        table = reference.replace('"', "").lower()
        if table not in ALLOWED_TABLES and table not in cte_names:
            raise UnsafeSQL(f"the table '{table}' is not allowed; use nfip_claims")

    return f"select * from (\n{statement}\n) limit {MAX_ROWS}"
