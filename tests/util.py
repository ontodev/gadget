import csv
import os
import pytest

from sqlalchemy import create_engine
from tabulate import tabulate

POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PW = os.environ.get("POSTGRES_PW", "postgres")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
postgres_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PW}@{POSTGRES_HOST}:{POSTGRES_PORT}/gadget_test"

sqlite_url = "sqlite:///" + os.path.abspath("build/obi.db")


def add_statement_table(conn, table_name):
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(
        f"""CREATE TABLE {table_name} (
            assertion INT NOT NULL,
            retraction INT NOT NULL DEFAULT 0,
            graph TEXT NOT NULL,
            subject TEXT NOT NULL,
            predicate TEXT NOT NULL,
            object TEXT NOT NULL,
            datatype TEXT NOT NULL,
            annotation TEXT
        )"""
    )
    with open(f"tests/resources/{table_name}.tsv") as f:
        rows = []
        for row in csv.reader(f, delimiter="\t"):
            rows.append([None if not x else x for x in row])
        for r in rows:
            query = []
            for itm in r:
                if not itm:
                    query.append("NULL")
                    continue
                query.append("'" + itm.replace("'", "''").replace("%", "%%") + "'")
            query = ", ".join(query)
            conn.execute(f"INSERT INTO {table_name} VALUES ({query})")


def add_tables(conn):
    with conn.begin():
        conn.execute("DROP TABLE IF EXISTS prefix")
        conn.execute(
            "CREATE TABLE prefix (prefix TEXT PRIMARY KEY NOT NULL, base TEXT NOT NULL)"
        )
        with open("tests/resources/prefix.tsv") as f:
            rows = list(csv.reader(f, delimiter="\t"))
            for r in rows:
                conn.execute(f"INSERT INTO prefix VALUES ('{r[0]}', '{r[1]}')")
        add_statement_table(conn, "statement")
        add_statement_table(conn, "extract_no_hierarchy")
        add_statement_table(conn, "extract_with_ancestors")
        add_statement_table(conn, "extract_with_ancestors_no_intermediates")


def compare_tables(conn, table_name):
    results = conn.execute(
        f"""SELECT subject, predicate, object, datatype, "expected" AS source
            FROM (SELECT * FROM {table_name}
                           EXCEPT
                           SELECT * FROM test_{table_name})
            UNION ALL
            SELECT subject, predicate, object, datatype, "actual" AS source
            FROM (SELECT * FROM test_{table_name}
                           EXCEPT
                           SELECT * FROM {table_name})"""
    ).fetchall()
    if results:
        expected = []
        actual = []
        for res in results:
            res = dict(res)
            s = res["source"]
            del res["source"]
            if s == "expected":
                expected.append(res.values())
            else:
                actual.append(res.values())
        if expected:
            print(f"\n{len(expected)} rows missing from test output:\n")
            print(tabulate(expected, headers=["subject", "predicate", "object", "datatype"]))
            print()
        if actual:
            print(f"\n{len(actual)} extra rows in test output:\n")
            print(tabulate(actual, headers=["subject", "predicate", "object", "datatype"]))
            print()
        pytest.fail("test output differs from expected output")


@pytest.fixture
def create_postgresql_db():
    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PW}@{POSTGRES_HOST}:{POSTGRES_PORT}",
        isolation_level="AUTOCOMMIT",
    )
    with engine.connect() as conn:
        res = conn.execute(
            "SELECT datname FROM pg_database WHERE datname = 'gadget_test';"
        ).fetchone()
        if not res:
            with conn.begin():
                conn.execute("CREATE DATABASE gadget_test")
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        add_tables(conn)


@pytest.fixture
def create_sqlite_db():
    if not os.path.isdir("build"):
        os.mkdir("build")
    engine = create_engine(sqlite_url)
    with engine.connect() as conn:
        add_tables(conn)
