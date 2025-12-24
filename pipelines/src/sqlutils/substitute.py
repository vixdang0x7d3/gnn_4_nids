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
    - Integrates with SQL class from sqlutils.core

Warning:
    Identifier substitution is inherently riskier than parameter binding. Only use
    this for trusted identifiers (table names, column names) that cannot be bound
    as parameters. Never substitute user input directly without validation.

Example:
    Using SQLTemplate class (recommended):
        >>> from sqlutils import SQLTemplate
        >>> template = SQLTemplate("SELECT * FROM ${schema}.${table} WHERE id = :user_id")
        >>> sql = template.substitute(schema="public", table="users")
        >>> bound = sql(user_id=123)
        >>> query, params = bound.duck

    Loading from file:
        >>> template = SQLTemplate.from_file("queries/get_users.sql")
        >>> sql = template.substitute(table="users", schema="public")

    With whitelist:
        >>> allowed = {"users", "orders", "products"}
        >>> template = SQLTemplate("SELECT * FROM ${table}", whitelist=allowed)
        >>> sql = template.substitute(table="users")

Functions:
    is_safe_identifier: Validate if a string is a safe SQL identifier.
    substitute_identifiers: Substitute ${var} placeholders with validated identifiers.

Classes:
    SQLTemplate: Template wrapper for SQL with ${identifier} placeholders.
"""

import re
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlutils.core import SQL


def is_safe_identifier(name: str) -> bool:
    """
    Check if identifier is safe (basic sanity check).
    Only alphanumeric, underscore, dot, and comma allowed.
    Allows schema.table notation and comma-separated lists.

    Args:
        name: Identifier string to validate

    Returns:
        True if identifier is safe, False otherwise

    Examples:
        >>> is_safe_identifier("users")
        True
        >>> is_safe_identifier("public.users")
        True
        >>> is_safe_identifier("id, name, email")
        True
        >>> is_safe_identifier("users; DROP TABLE")
        False
    """
    if not isinstance(name, str) or not name:
        return False

    # Allow comma-separated lists for column names
    # Each part should be a valid identifier or schema.table
    parts = [p.strip() for p in name.split(",")]

    for part in parts:
        # Pattern: word or schema.table or table.column
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$"
        if not re.match(pattern, part):
            return False

    return True


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


class SQLTemplate:
    """
    Template for SQL with ${identifier} placeholders.

    This class wraps SQL templates that contain ${var} placeholders for identifiers
    (table names, column names, etc.) that need to be substituted before query execution.

    Attributes:
        dialect: SQL dialect for parsing (default: "duckdb")
        keep_comments: Whether to preserve SQL comments (default: False)
        whitelist: Optional set of allowed identifier values for security

    Examples:
        Basic usage:
            >>> template = SQLTemplate("SELECT * FROM ${table} WHERE id = :user_id")
            >>> sql = template.substitute(table="users")
            >>> bound = sql(user_id=123)
            >>> query, params = bound.duck

        From file:
            >>> template = SQLTemplate.from_file("queries/dynamic.sql")
            >>> sql = template.substitute(schema="public", table="users")

        With whitelist:
            >>> allowed = {"users", "orders", "products"}
            >>> template = SQLTemplate(
            ...     "SELECT * FROM ${table}",
            ...     whitelist=allowed
            ... )
            >>> sql = template.substitute(table="users")  # OK
            >>> sql = template.substitute(table="admin")  # ValueError
    """

    def __init__(
        self,
        template: str,
        dialect: str = "duckdb",
        keep_comments: bool = False,
        whitelist: set[str] | None = None,
    ):
        """
        Initialize SQL template.

        Args:
            template: SQL string with ${var} placeholders
            dialect: SQL dialect for parsing (default: "duckdb")
            keep_comments: Whether to preserve comments (default: False)
            whitelist: Optional set of allowed identifier values
        """
        self._template = template
        self.dialect = dialect
        self.keep_comments = keep_comments
        self.whitelist = whitelist

    @classmethod
    def from_file(
        cls,
        filepath: str | Path,
        dialect: str = "duckdb",
        keep_comments: bool = False,
        whitelist: set[str] | None = None,
    ) -> "SQLTemplate":
        """
        Load SQL template from file.

        Args:
            filepath: Path to SQL file
            dialect: SQL dialect for parsing (default: "duckdb")
            keep_comments: Whether to preserve comments (default: False)
            whitelist: Optional set of allowed identifier values

        Returns:
            SQLTemplate instance

        Example:
            >>> template = SQLTemplate.from_file("queries/get_users.sql")
            >>> sql = template.substitute(table="users")
        """
        content = Path(filepath).read_text()
        return cls(
            content,
            dialect=dialect,
            keep_comments=keep_comments,
            whitelist=whitelist,
        )

    def substitute(
        self,
        identifiers: dict[str, str] | None = None,
        validate_safety: bool = True,
        **kwargs: str,
    ) -> "SQL":
        """
        Substitute identifiers and return a SQL object.

        Args:
            identifiers: Dict mapping placeholder names to identifiers
            validate_safety: If True, validate identifiers are safe (default: True)
            **kwargs: Additional identifiers as keyword arguments

        Returns:
            SQL object with identifiers substituted, ready for parameter binding

        Raises:
            ValueError: If identifier validation fails or whitelist check fails

        Examples:
            Dict style:
                >>> template = SQLTemplate("SELECT * FROM ${table}")
                >>> sql = template.substitute({"table": "users"})

            Kwargs style:
                >>> sql = template.substitute(table="users")

            Combined:
                >>> sql = template.substitute({"table": "users"}, schema="public")
        """
        from sqlutils.core import SQL

        # Merge dict and kwargs
        ids = identifiers or {}
        ids.update(kwargs)

        # Perform substitution
        resolved = substitute_identifiers(
            self._template,
            ids,
            whitelist=self.whitelist,
            validate_safety=validate_safety,
        )

        # Return SQL object ready for parameter binding
        return SQL(resolved, dialect=self.dialect, keep_comments=self.keep_comments)

    @property
    def template(self) -> str:
        """Get the raw template string."""
        return self._template

    def __repr__(self) -> str:
        """String representation of the template."""
        preview = (
            self._template[:50] + "..." if len(self._template) > 50 else self._template
        )
        whitelist_info = (
            f", whitelist={len(self.whitelist)} items" if self.whitelist else ""
        )
        return f"SQLTemplate({preview!r}{whitelist_info})"
