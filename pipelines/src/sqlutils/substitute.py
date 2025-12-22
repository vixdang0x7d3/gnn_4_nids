"""
Safe SQL identifier substitution utilities.

This module provides utilities for safely substituting table names, column names,
and other SQL identifiers into query templates. Unlike parameter binding (which
handles values), this module handles structural parts of queries that cannot be
parameterized.

Security:
    - Validates identifiers match safe patterns (alphanumeric + underscore + dot)
    - Optional whitelist enforcement for production environments
    - Prevents SQL injection through identifier validation
    - Uses string.Template for ${var} substitution syntax

Warning:
    Identifier substitution is inherently riskier than parameter binding. Only use
    this for trusted identifiers (table names, column names) that cannot be bound
    as parameters. Never substitute user input directly without validation.

Example:
    Dynamic table selection:
        >>> sql = "SELECT * FROM ${schema}.${table} WHERE id = :user_id"
        >>> safe_sql = substitute_identifiers(
        ...     sql,
        ...     {"schema": "public", "table": "users"}
        ... )
        >>> # Result: "SELECT * FROM public.users WHERE id = :user_id"

    With whitelist for production:
        >>> allowed_tables = {"users", "orders", "products"}
        >>> substitute_identifiers(
        ...     "SELECT * FROM ${table}",
        ...     {"table": "users"},
        ...     whitelist=allowed_tables
        ... )

    Multi-column dynamic queries:
        >>> sql = "SELECT ${cols} FROM users ORDER BY ${order_by}"
        >>> substitute_identifiers(
        ...     sql,
        ...     {"cols": "id, name, email", "order_by": "created_at"}
        ... )

Functions:
    is_safe_identifier: Validate if a string is a safe SQL identifier.
    substitute_identifiers: Substitute ${var} placeholders with validated identifiers.
"""

import re
from string import Template


def is_safe_identifier(name: str) -> bool:
    """
    Check if identifier is safe (basic sanity check).
    Only alphanumeric, underscore, and dot allowed.
    Allows schema.table notation.
    """
    if not isinstance(name, str) or not name:
        return False

    pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$"
    return bool(re.match(pattern, name))


def substitute_identifiers(
    sql: str,
    identifiers: dict[str, str],
    whitelist: set[str] | None = None,
    validate_safety: bool = True,
) -> str:
    """
    Substitute ${var} identifiers in SQL with validation.

    Args:
        sql: SQL string with ${var} placeholders for identifiers
        identifiers: dict mapping placeholder names to actual identifiers
        whitelist: set of allowed identifier values (if None, skip whitelist check)
        validate_safety: If True, check that values are safe identifiers

    Returns:
        SQL string with identifiers substituted

    Raises:
        ValueError: If validation fails

    Example:
        >>> substitute_identifiers(
        ...     "SELECT * FROM ${table} WHERE ${col} = :value",
        ...     {"table": "users", "col": "id"}
        ... )
        "SELECT * FROM users WHERE id = :value"
    """
    # Validate identifier values (not placeholder names)
    for placeholder, value in identifiers.items():
        if validate_safety and not is_safe_identifier(value):
            raise ValueError(
                f"Unsafe identifier value: '{value}' for placeholder '{placeholder}'"
            )

        if whitelist is not None and value not in whitelist:
            raise ValueError(f"Identifier '{value}' not in whitelist")

    # Use substitute() instead of safe_substitute() to error on missing vars
    return Template(sql).substitute(**identifiers)
