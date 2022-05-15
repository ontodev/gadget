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


def add_table(conn, table_name):
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    with open(f"tests/resources/{table_name}.tsv") as f:
        reader = csv.reader(f, delimiter="\t")
        headers = ", ".join([x.replace(":", "_") for x in next(reader)])
        conn.execute(f"CREATE TABLE {table_name} ({headers})")
        for row in reader:
            query = []
            for itm in row:
                if not itm:
                    query.append("NULL")
                    continue
                query.append("'" + itm.replace("'", "''").replace("%", "%%") + "'")
            query = ", ".join(query)
            conn.execute(f"INSERT INTO {table_name} VALUES ({query})")


def add_tables(conn):
    with conn.begin():
        conn.execute("DROP TABLE IF EXISTS prefix")
        conn.execute("CREATE TABLE prefix (prefix TEXT PRIMARY KEY NOT NULL, base TEXT NOT NULL)")
        with open("tests/resources/prefix.tsv") as f:
            rows = list(csv.reader(f, delimiter="\t"))
            for r in rows:
                conn.execute(f"INSERT INTO prefix VALUES ('{r[0]}', '{r[1]}')")
        for t in os.listdir("tests/resources"):
            if t == "prefix.tsv":
                continue
            add_table(conn, os.path.splitext(t)[0])


def compare_lines(actual_lines, expected_lines):
    removed = list(set(expected_lines) - set(actual_lines))
    added = list(set(actual_lines) - set(expected_lines))
    removed = [f"---\t{x}" for x in removed if x != ""]
    added = [f"+++\t{x}" for x in added if x != ""]
    diff = removed + added
    if diff:
        print("The actual and expected outputs differ:\n")
        for line in diff:
            print(line)
        pytest.fail()


def compare_tables(conn, table_name):
    results = conn.execute(
        f"""SELECT *, "expected" AS source
            FROM (SELECT * FROM {table_name}
                   EXCEPT
                   SELECT * FROM test_{table_name})
            UNION ALL
            SELECT *, "actual" AS source
            FROM (SELECT * FROM test_{table_name}
                   EXCEPT
                   SELECT * FROM {table_name})"""
    ).fetchall()
    import logging
    if results:
        expected = []
        actual = []
        for res in results:
            res = dict(res)
            logging.error(res)
            s = res["source"]
            del res["source"]
            if s == "expected":
                expected.append(res.values())
            else:
                actual.append(res.values())
        if expected:
            print(f"\n{len(expected)} rows missing from test output:\n")
            print(tabulate(expected, headers=results[0].keys()))
            print()
        if actual:
            print(f"\n{len(actual)} extra rows in test output:\n")
            print(tabulate(actual, headers=results[0].keys()))
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
