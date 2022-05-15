import csv

from gadget.export import export
from io import StringIO
from sqlalchemy import create_engine
from util import create_sqlite_db, compare_tables, sqlite_url


def export_with_predicates(conn):
    tsv = export(conn, predicates=["CURIE", "label", "definition"], terms=["OBI:0100046"])
    load_tsv(conn, "test_export_with_predicates", tsv)


def export_no_predicates(conn):
    tsv = export(conn, default_value_format="CURIE", terms=["OBI:0100046"])
    load_tsv(conn, "test_export_no_predicates", tsv)
    compare_tables(conn, "export_no_predicates")


def load_tsv(conn, table, tsv):
    io = StringIO(tsv)
    reader = csv.reader(io, delimiter="\t")
    headers = next(reader)

    conn.execute(f"DROP TABLE IF EXISTS {table}")
    query = f'CREATE TABLE "{table}" ('
    query += ", ".join([h.replace(":", "_") for h in headers])
    query += ")"
    conn.execute(query)

    values = []
    for row in reader:
        values.append("(" + ", ".join([f"'{x}'" if x else "NULL" for x in row]) + ")")
    query = f'INSERT INTO {table} VALUES '
    query += ", ".join(values)
    conn.execute(query)


def test_export_sqlite(create_sqlite_db):
    engine = create_engine(sqlite_url)
    with engine.connect() as conn:
        export_no_predicates(conn)
        export_with_predicates(conn)
