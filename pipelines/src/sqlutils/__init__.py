"""
sqlutils - SQL queries as immutable objects with parameter binding and validation

Core features:
- Immutable SQL query objects with AST-based parsing
- Named parameter binding with `:placeholder` syntax
- Query introspection (tables, parameters, query type)
- Safe identifier substitution with `${identifier}` syntax
- DuckDB integration
"""

from .core import SQL, BoundSQL, BulkBoundSQL
from .substitute import SQLTemplate, is_safe_identifier, substitute_identifiers

__version__ = "0.1.0"

__all__ = [
    "SQL",
    "BoundSQL",
    "BulkBoundSQL",
    "SQLTemplate",
    "is_safe_identifier",
    "substitute_identifiers",
]
