from gadget.extract import extract
from util import compare_tables, create_postgresql_db, create_sqlite_db, sqlite_url

from sqlalchemy import create_engine


def extract_no_hierarchy(conn):
    extract(
        conn,
        extract_table="test_extract_no_hierarchy",
        no_hierarchy=True,
        predicates=["rdfs:label", "BFO:0000179"],
        terms={"OBI:0100046": {}, "BFO:0000040": {}}
    )
    compare_tables(conn, "extract_no_hierarchy")


def extract_with_ancestors(conn):
    extract(
        conn,
        extract_table="test_extract_with_ancestors",
        predicates=["rdfs:label", "BFO:0000179"],
        terms={"OBI:0100046": {"Related": "ancestors"}}
    )
    compare_tables(conn, "extract_with_ancestors")


def extract_with_ancestors_no_intermediates(conn):
    extract(
        conn, extract_table="test_extract_with_ancestors_no_intermediates", intermediates="none", predicates=["rdfs:label"], terms={"OBI:0100046": {"Related": "ancestors"}, "OBI:0000666": {"Related": "ancestors"}})
    compare_tables(conn, "extract_with_ancestors_no_intermediates")


def test_extract_sqlite(create_sqlite_db):
    engine = create_engine(sqlite_url)
    with engine.connect() as conn:
        extract_no_hierarchy(conn)
        extract_with_ancestors(conn)
        extract_with_ancestors_no_intermediates(conn)
