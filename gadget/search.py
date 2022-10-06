from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from typing import List, Optional
from .sql import MAX_SQL_VARS


def search(
    conn: Connection,
    limit: Optional[int] = None,
    search_text: str = None,
    statement: str = "statement",
    term_ids: List[str] = None,
) -> List[dict]:
    """Use the statement_search_view table to return matching search results (from search_text).

    :param conn: database connection to query
    :param limit: max number of search results to return
    :param search_text: text to search (matching label, synonym, or ID)
    :param statement: name of ontology statement table
    :param term_ids: list of term IDs to restrict search results to
    :return: list of matching terms in typeahead dict format (id, label, order)
    """
    if not search_text and not term_ids:
        return []
    if not search_text:
        # Make sure not to insert None into search
        search_text = ""
    query = f"""SELECT DISTINCT subject, label FROM {statement}_search_view
            WHERE LOWER(label) LIKE :search_text"""
    results = []
    if term_ids:
        query += " AND subject IN :term_ids"
        if limit:
            query += f" LIMIT {limit}"

        query = f"SELECT subject, label FROM ({query}) AS t ORDER BY LENGTH(label)"
        query = sql_text(query).bindparams(bindparam("term_ids", expanding=True))
        # Use chunks to get around max SQL variables
        chunks = [term_ids[i : i + MAX_SQL_VARS] for i in range(0, len(term_ids), MAX_SQL_VARS)]
        for chunk in chunks:
            results.extend(
                conn.execute(
                    query, search_text=f"%%{search_text.lower()}%%", term_ids=chunk
                ).fetchall()
            )
    else:
        if limit:
            query += f" LIMIT {limit}"
        query = f"SELECT subject, label FROM ({query}) AS t ORDER BY LENGTH(label)"
        results = conn.execute(sql_text(query), search_text=f"%%{search_text.lower()}%%").fetchall()
    return [
        {"id": res["subject"], "label": res["label"], "order": i}
        for i, res in enumerate(results, 1)
    ]
