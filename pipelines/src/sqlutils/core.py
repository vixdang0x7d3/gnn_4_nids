from pathlib import Path
from typing import Any, Union

from sqlglot import exp, parse_one
from sqlglot.expressions import replace_placeholders


class SQL:
    def __init__(
        self,
        query: str,
        dialect: str = "duckdb",
        keep_comments: bool = False,
    ):
        self._ast = parse_one(query, dialect=dialect)
        self.dialect = dialect
        self.keep_comments = keep_comments

    @property
    def template(self) -> str:
        return self._ast.sql(
            dialect=self.dialect,
            comments=self.keep_comments,
            pretty=True,
        )

    @property
    def duck(self) -> tuple[str, None]:
        if self.placeholder_count() > 0:
            raise RuntimeError("SQL contains placeholders, must provide parameters")
        return self.template, None

    def placeholder_count(self) -> int:
        """Count the number of placeholders in the SQL query."""
        return len(list(self._ast.find_all(exp.Placeholder)))

    @classmethod
    def from_file(
        cls,
        filepath: str | Path,
        dialect: str = "duckdb",
        keep_comments: bool = False,
    ) -> "SQL":
        """
        Load sql from file
        """
        content = Path(filepath).read_text()
        return cls(
            content,
            dialect=dialect,
            keep_comments=keep_comments,
        )

    def __call__(self, *args, **kwargs) -> Union["BoundSQL", "BulkBoundSQL"]:
        if args and isinstance(args[0], list):
            return BulkBoundSQL(self, args[0])
        elif args and isinstance(args[0], dict):
            return BoundSQL(self, args[0])
        elif kwargs:
            return BoundSQL(self, kwargs)
        else:
            raise ValueError(
                "Invalid parameters."
                " Acceptable parameter types: dict[str, Any], list[tuple], kwargs"
            )


class BoundSQL:
    """Single row bound SQL statement"""

    def __init__(self, sql: SQL, params: dict[str, Any]):
        self.obj = sql
        self.params = params

        self._validate_params()

    def _validate_params(self):
        expected = self.obj.placeholder_count()
        actual = len(self.params)

        if expected != actual:
            raise ValueError(
                f"Parameter count mismatch: expected {expected}, got {actual}"
            )

    @property
    def duck(self) -> tuple[str, dict[str, Any] | None]:
        def duckified(node):
            if isinstance(node, exp.Placeholder) and node.name != "?":
                return exp.Identifier(this=f"${node.name}")
            return node

        return self.obj._ast.transform(duckified).sql(
            dialect=self.obj.dialect, comments=self.obj.keep_comments, pretty=True
        ), self.params

    @property
    def literal(self) -> tuple[str, None]:
        bound_ast = self._bind_to_ast()
        return (
            bound_ast.sql(
                dialect=self.obj.dialect,
                comments=self.obj.keep_comments,
                pretty=True,
            ),
            None,
        )

    def _bind_to_ast(self):
        """Internal: bind to AST, replacing placeholders"""
        ast = self.obj._ast.copy()
        replace_placeholders(ast, *self.params.values())

        return ast

    def __repr__(self):
        return f"BoundSQL({self.obj.template}, {self.params})"


class BulkBoundSQL:
    """Batch bound SQL statement"""

    def __init__(
        self,
        sql: SQL,
        rows: list[tuple],
    ):
        self.obj = sql
        self.rows = rows
        self._validate_rows()

    def _validate_rows(self):
        expected = self.obj.placeholder_count()
        for i, row in enumerate(self.rows):
            actual = len(row)
            if len(row) != expected:
                raise ValueError(f"Row {i}: expected {expected}, got {actual}")

    @property
    def duck(self) -> tuple[str, list]:
        rows = [list(r) for r in self.rows]
        return (self.obj.template, rows)

    @property
    def literal(self) -> tuple[str, None]:
        raise NotImplementedError("BulkBatchSQL has no literal representation")

    def __repr__(self) -> str:
        return f"BulkBoundSQL({self.obj.template[:50]}, {len(self.rows)} rows)"
