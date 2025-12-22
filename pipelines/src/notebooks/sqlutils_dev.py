import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")

with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo
    import duckdb

    from sqlglot import exp, parse_one
    from sqlglot.expressions import replace_placeholders


@app.cell
def _():
    conn = duckdb.connect()
    conn.install_extension("tpch")
    conn.sql("""
    call dbgen(sf=1)
    """)
    return (conn,)


@app.cell
def _(conn):
    _df = mo.sql(
        f"""
        SHOW TABLES
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, customer):
    _df = mo.sql(
        f"""
        SUMMARIZE customer
        """,
        engine=conn
    )
    return


@app.cell
def _():
    query = """
    -- Select all from customer
    SELECT * FROM customer 
    where c_custkey=42 --  whose id is 42
    """

    named_placeholder_query = """
    select stime, proto, state, service
    from raw_conn
    where stime >= :latest_stime
      and last_mod >= :lastest_last_mod
    """

    positional_placeholder_query = """
    insert into raw_conn values(?, ?, ?)
    """


    expression = parse_one(query)
    expression_1 = parse_one(named_placeholder_query)
    expression_2 = parse_one(positional_placeholder_query)
    return expression, expression_1, expression_2, positional_placeholder_query


@app.cell
def _(expression):
    # Strip comments, formatting
    print(expression.sql(comments=False, pretty=True))
    return


@app.cell
def _(expression_1):
    for p in expression_1.find_all(exp.Placeholder):
        print(p.name)
    return


@app.cell
def _(expression_2):
    for _p in expression_2.find_all(exp.Placeholder):
        print(_p)
    return


@app.cell
def _(expression_2):
    replace_placeholders(
        expression_2,
        *(1, "Bob", False, 2.0, 2.23),
        *(1, "Bob", False, 2.0, 2.23),
        *(1, "Bob", False, 2.0, 2.23),
    ).sql()
    return


@app.cell
def _(expression_1):
    def duckified(node):
        if isinstance(node, exp.Placeholder) and node.name != '?':
            return exp.Identifier(this=f"${node.name}")
        return node

    expression_1.transform(duckified).sql()
    return


@app.cell
def _(positional_placeholder_query):
    pos_exp = parse_one(positional_placeholder_query)

    data = [(1, "Bob", 42.1), (2, "Jim", 21.2), (1, "Cal", 12.1)]

    _expression = exp.Values(
        expressions=[
            exp.Tuple(
                expressions=[
                    exp.Tuple(expressions=[exp.convert(v) for v in row])
                    for row in data
                ]
            )
        ]
    )

    _expression.sql()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### SQLUtils tests
    """)
    return


@app.cell
def _():
    from sqlutils import SQL


    insert_stmt = SQL("""
    insert into user values(?, ?, ?)
    """)

    print(insert_stmt.template)
    return SQL, insert_stmt


@app.cell
def _(insert_stmt):
    test_conn = duckdb.connect()
    test_conn.sql("""
    create table if not exists user (
        id BIGINT,
        name VARCHAR,
        points DOUBLE
    )
    """)

    _data = [(1, "Bob", 42.1), (2, "Jim", 21.2), (1, "Cal", 12.1)]
    bound = insert_stmt(_data)

    test_conn.executemany(*bound.duck)
    return (test_conn,)


@app.cell
def _(test_conn, user):
    _df = mo.sql(
        f"""
        from user
        """,
        engine=test_conn
    )
    return


@app.cell
def _(SQL, conn):
    customer_info_query = "select * from customer where c_custkey = :key"
    templ = SQL(customer_info_query)

    _query = templ(key=2)

    conn.execute(*_query.duck)
    conn.fetchone()
    return


@app.cell
def _(SQL):
    select_query = SQL.from_file("src/sql/features/get_last_computed_stime.sql")

    select_query.duck
    return


if __name__ == "__main__":
    app.run()
