import re
from pathlib import Path
from typing import Any

from sqlglot import exp, parse_one


class SQL:
    """Immutable SQL query object with convenients built-in"""

    def __init__(
        self,
        query: str,
        params: dict[str, Any] | None = None,
        dialect: str = "duckdb",
        batch_params: list[tuple] | None = None,
    ):
        self.query = query.strip()
        self.params = params or {}
        self.batch_params = batch_params
        self.dialect = dialect
        self._ast = parse_one(self.query, dialect=self.dialect)

    @property
    def duck(self) -> tuple[str, dict[str, Any] | None]:
        """Return (query, params) with DuckDB-style $placeholders for con.execute(*sql.duck)"""
        query = self.query.replace(":", "$")
        return (query, self.params if self.params else None)

    @property
    def duck_many(self) -> tuple[str, list[tuple] | None]:
        """
        Return (query, param_sets) for DuckDB batch execution.

        For excecutemany():
            cursor.executemany(*sql.duck_many)
        """
        style = self.placehodler_style()

        if style == "postional":
            return (self.query, self.batch_params)
        elif style == "named":
            query = self.query.replace(":", "$")
            return (query, self.batch_params)
        else:
            raise ValueError(f"Cannot use duck_many with {style} placehodlers")

    def has_batch_params(self) -> bool:
        """Check if SQL object has batch parameters bound"""
        return self.batch_params is not None

    def batch_size(self) -> int:
        """Return the number of parameter sets in batch (0 if not batch)"""
        return len(self.batch_params) if self.batch_params else 0

    def __call__(self, **params) -> "SQL":
        """Bind parameters (merges with existing)"""
        new_params = self.params | params
        return SQL(self.query, new_params, self.dialect)

    def bind(self, **params) -> "SQL":
        """Bind parameters (alias for __call__)"""
        return self(**params)

    def bind_many(self, param_set: list[tuple]) -> "SQL":
        """
        Bind multiple parameter sets for batch operations
        (executemany).

        Args:
            param_sets: List of tuples (positional params)

        Returns:
            New SQL instance with batch params

        Example:
            sql = SQL("INSERT INTO users VALUES (?, ?)")
            sql_batch = sql.bind_many([('alice' , 25), ('bob', 30)])
            cursor.executemany(*sql_batch.duck_many)
        """
        return SQL(
            self.query,
            params=None,
            dialect=self.dialect,
            batch_params=param_set,
        )

    def placehodler_style(self) -> str:
        """
        Detect placeholder style in query.
        Returns: 'named' (:param), 'positional' (?), 'mixed', 'none'
        """
        has_named = bool(self.all_params())
        has_positional = "?" in self.query

        if has_named and has_positional:
            return "mixed"
        elif has_named:
            return "named"
        elif has_positional:
            return "positional"
        else:
            return "none"

    def all_params(self) -> set[str]:
        """Extract parameter names using sqlglot"""
        params = set()
        for placeholder in self._ast.find_all(exp.Placeholder):
            if placeholder.name:
                params.add(placeholder.name)
        return params

    def is_fully_bound(self) -> bool:
        """Check if all parameters are bound"""
        required = self.all_params()
        return required.issubset(set(self.params.keys()))

    def bound(self) -> bool:
        """Alias for is_fully_bound()"""
        return self.is_fully_bound()

    def missing_params(self) -> set[str]:
        """Return parameters that are not yet bound"""
        return self.all_params() - set(self.params.keys())

    def query_type(self) -> str:
        """Get query type (SELECT, INSERT, etc.)"""
        return self._ast.key.upper()

    def is_readonly(self) -> bool:
        """Check if query only reads data"""
        return self.query_type() in ("SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN")

    def tables(self) -> set[str]:
        """Extract table names"""
        return {t.name for t in self._ast.find_all(exp.Table)}

    @classmethod
    def from_file(
        cls,
        filepath: str | Path,
        dialect: str = "duckdb",
        strip_comments: bool = True,
    ) -> "SQL":
        """
        Load and format SQL from file.

        Args:
            filepath (str | Path): Path to SQL file.
            dialect (str, optional): SQL dialect. Defaults to "duckdb".
            strip_comments (bool, optional): Strip SQL comments. Defaults to True.

        Returns:
            SQL: SQL object with formatted query.
        """
        content = Path(filepath).read_text(encoding="utf-8")

        if strip_comments:
            content = cls._strip_sql_comments(content)

        ast = parse_one(content, dialect=dialect)
        formatted = ast.sql(dialect=dialect, pretty=True)

        return cls(query=formatted, dialect=dialect)

    @staticmethod
    def _strip_sql_comments(sql: str) -> str:
        """
        Strip SQL comments from query string.

        Removes:
        - Single-line comments (-- comment)
        - Multi-line comments (/* comment */)
        """

        # Remove multi-line comments /* ... */
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)

        lines = []
        for line in sql.split("\n"):
            if "--" in line:
                quote_count = line[: line.find("--")].count("'")
                if quote_count % 2 == 0:  # Even quotes = not in string
                    line = line[: line.find("--")]
            lines.append(line)

        sql = "\n".join(lines)

        # Remove extra blank lines
        sql = re.sub(r"\n\s*\n+", "\n\n", sql)

        return sql.strip()

    def _value_to_literal(self, value: Any) -> exp.Expression:
        """Convert Python value to sqlglot literal node"""
        if isinstance(value, bool):
            return exp.Boolean(this=value)
        elif isinstance(value, (list, tuple)):
            elements = [self._value_to_literal(v) for v in value]
            return exp.Array(expressions=elements)
        elif isinstance(value, str):
            return exp.Literal.string(value)
        elif isinstance(value, (int, float)):
            return exp.Literal.number(value)
        elif value is None:
            return exp.Null()
        else:
            return exp.Literal.string(str(value))

    def _substitute_params(self) -> str:
        """Substitute parameters into query using sqlglot"""
        if not self.params:
            return self._ast.sql(dialect=self.dialect, pretty=True)

        # Clone AST to avoid mutating original
        ast = self._ast.copy()

        # Replace placeholders with literal values
        for placeholder in ast.find_all(exp.Placeholder):
            if placeholder.name and placeholder.name in self.params:
                literal = self._value_to_literal(self.params[placeholder.name])
                placeholder.replace(literal)

        return ast.sql(dialect=self.dialect, pretty=True)

    def __str__(self) -> str:
        """Return formatted query with substituted parameters"""
        return self._substitute_params()

    def __repr__(self) -> str:
        """Developer representation with substituted query"""
        substituted = self._substitute_params()
        preview = substituted[:100] + "..." if len(substituted) > 100 else substituted
        bound_status = (
            "bound" if self.is_fully_bound() else f"missing {self.missing_params()}"
        )
        return f"SQL('{preview}', {bound_status})"
