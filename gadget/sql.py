import os
import logging

from collections import defaultdict
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from typing import Dict, List, Tuple

MAX_SQL_VARS = os.environ.get("MAX_SQL_VARS") or 999
TOP_LEVELS = {
    "owl:Ontology": "Ontology",
    "owl:Class": "Class",
    "owl:AnnotationProperty": "Annotation Property",
    "owl:DatatypeProperty": "Data Property",
    "owl:ObjectProperty": "Object Property",
    "owl:Individual": "Individual",
    "rdfs:Datatype": "Datatype",
}


def get_ancestor_hierarchy(
    conn: Connection, term_id: str, statement="statement", sub_class: bool = False
) -> dict:
    """Return a dict of child -> list of parents for the full ancestor lineage of the given term.

    :param conn: database connection to query
    :param term_id: term to get ancestors of
    :param statement: name of the ontology statement table
    :param sub_class: if True, substitute owl:Class for owl:Thing (support for tree view)
    :return: dict of child -> set of parents
    """
    query = sql_text(
        f"""WITH RECURSIVE ancestors(parent, child) AS (
        VALUES (:term_id, NULL)
        UNION
        -- The children of the given term:
        SELECT object AS parent, subject AS child
        FROM "{statement}"
        WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
          AND object = :term_id
          AND datatype = '_IRI'
        UNION
        --- Children of the children of the given term
        SELECT object AS parent, subject AS child
        FROM "{statement}"
        WHERE object IN (SELECT subject FROM "{statement}"
                         WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
                         AND object = :term_id)
          AND predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
          AND datatype = '_IRI'
        UNION
        -- The non-blank parents of all of the parent terms extracted so far:
        SELECT object AS parent, subject AS child
        FROM "{statement}", ancestors
        WHERE ancestors.parent = "{statement}".subject
          AND "{statement}".predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
          AND "{statement}".datatype = '_IRI'
      )
      SELECT * FROM ancestors"""
    )
    results = conn.execute(query, term_id=term_id).fetchall()
    ancestors = defaultdict(list)
    for res in results:
        parent = res["parent"]
        if parent == "owl:Thing":
            parent = "owl:Class"
        if res["child"] not in ancestors:
            ancestors[res["child"]] = []
        ancestors[res["child"]].append(parent)
    return ancestors


def get_children(conn: Connection, term_id: str, statement: str = "statement"):
    query = sql_text(
        f"""SELECT DISTINCT subject FROM "{statement}"
        WHERE object = :term_id AND predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')"""
    )
    results = conn.execute(query, term_id=term_id)
    return [x["subject"] for x in results]


def get_descendant_hierarchy(conn: Connection, term_id: str, statement: str = "statement"):
    query = sql_text(
        f"""WITH RECURSIVE descendants(child, parent) AS (
            VALUES (:term_id, NULL)
            UNION
            -- The children of the given term:
            SELECT subject AS child, object AS parent
            FROM "{statement}"
            WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
              AND object = :term_id
              AND datatype = '_IRI'
            UNION
            --- Children of the children of the given term
            SELECT subject AS child, object AS parent
            FROM "{statement}"
            WHERE object IN (SELECT subject FROM "{statement}"
                             WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
                             AND object = :term_id)
              AND predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
              AND datatype = '_IRI'
            UNION
            -- The children of all of the child terms extracted so far:
            SELECT subject AS child, object AS parent
            FROM "{statement}", descendants
            WHERE descendants.child = "{statement}".object
              AND "{statement}".predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
              AND "{statement}".datatype = '_IRI'
          )
          SELECT * FROM descendants"""
    )
    results = conn.execute(query, term_id=term_id)
    descendants = defaultdict(list)
    for res in results:
        if res["parent"] not in descendants:
            descendants[res["parent"]] = []
        descendants[res["parent"]].append(res["child"])
    return descendants


def get_descendants(conn: Connection, term_id: str, statement: str = "statements") -> list:
    """Return a set of descendants (in no order) for a given term ID."""
    query = sql_text(
        f"""WITH RECURSIVE descendants(node) AS (
                VALUES (:term_id)
                UNION
                 SELECT subject AS node
                FROM "{statement}"
                WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
                  AND subject = :term_id
                UNION
                SELECT subject AS node
                FROM "{statement}", descendants
                WHERE descendants.node = "{statement}".object
                  AND "{statement}".predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf')
            )
            SELECT * FROM descendants"""
    )
    results = conn.execute(query, target=term_id)
    return [x["node"] for x in results]


def get_entity_types(
    conn: Connection, term_ids: List[str], statement="statement"
) -> Dict[str, set]:
    query = sql_text(
        f"""SELECT DISTINCT subject, object FROM "{statement}"
            WHERE subject IN :term_ids AND predicate = 'rdf:type'"""
    ).bindparams(bindparam("term_ids", expanding=True))
    results = conn.execute(query, term_ids=term_ids).fetchall()
    all_types = defaultdict(list)
    for res in results:
        term_id = res["subject"]
        if term_id not in all_types:
            all_types[term_id] = list()
        all_types[term_id].append(res["object"])

    entity_types = {}
    for term_id, e_types in all_types.items():
        if len(e_types) >= 1:
            entity_types[term_id] = set(e_types)
        else:
            # Determine if this has a parent class or property and use that to infer type
            entity_type = None
            query = sql_text(f'SELECT predicate FROM "{statement}" WHERE subject = :term_id')
            results = conn.execute(query, term_id=term_id)
            preds = [row["predicate"] for row in results]
            if "rdfs:subClassOf" in preds:
                entity_types[term_id] = {"owl:Class"}
            elif "rdfs:subPropertyOf" in preds:
                entity_types[term_id] = {"owl:AnnotationProperty"}
            if not entity_type:
                query = sql_text(f"SELECT predicate FROM {statement} WHERE object = :term_id")
                results = conn.execute(query, term_id=term_id)
                preds = [row["predicate"] for row in results]
                if "rdfs:subClassOf" in preds:
                    entity_types[term_id] = {"owl:Class"}
                elif "rdfs:subPropertyOf" in preds:
                    entity_types[term_id] = {"owl:AnnotationProperty"}
        # No type could be determined, set to owl:Class
        if term_id not in entity_types:
            entity_types[term_id] = {"owl:Class"}
    return entity_types


def get_ids(
    conn: Connection,
    id_or_labels: List[str] = None,
    id_type="subject",
    statement: str = "statement",
) -> list:
    """Create list of IDs from a given list of IDs or labels.

    :param conn: database connection to query
    :param id_or_labels: list of IDs or labels to return all IDs for
    :param id_type: type of IDs to return (subject or predicate)
    :param statement: name of the ontology statement table
    :return: list of IDs
    """
    if id_or_labels:
        query = sql_text(
            f"""SELECT DISTINCT subject FROM "{statement}"
            WHERE predicate = 'rdfs:label' AND object IN :id_or_labels
            UNION
            SELECT DISTINCT {id_type} FROM "{statement}" WHERE {id_type} IN :id_or_labels"""
        ).bindparams(bindparam("id_or_labels", expanding=True))
        results = conn.execute(query, id_or_labels=id_or_labels)
        return [res["subject"] for res in results]
    else:
        # Get all predicates
        results = conn.execute(f'SELECT DISTINCT {id_type} FROM "{statement}"').fetchall()
        return [res["predicate"] for res in results]


def get_labels(conn: Connection, term_ids: list, statement="statement") -> Dict[str, str]:
    """Return a dictionary of term ID -> label.

    :param conn: database connection to query
    :param term_ids: list of term IDs to get labels for
    :param statement: name of ontology statement table
    :return:
    """
    labels = {}
    # Use chunks to get around max SQL variables
    chunks = [term_ids[i : i + MAX_SQL_VARS] for i in range(0, len(term_ids), MAX_SQL_VARS)]
    for chunk in chunks:
        query = sql_text(
            f"""SELECT subject, object FROM "{statement}"
                WHERE subject IN :ids AND predicate = 'rdfs:label' AND object IS NOT NULL"""
        ).bindparams(bindparam("ids", expanding=True))
        results = conn.execute(query, {"ids": chunk})
        for res in results:
            labels[res["subject"]] = res["object"]
    return labels


def get_objects(
    conn: Connection,
    predicate_ids: List[str],
    exclude_json: bool = False,
    include_all_predicates: bool = True,
    statement: str = "statement",
    term_ids: List[str] = None,
) -> dict:
    """Get a dict of predicate ID -> objects.

    :param conn: database connection to query
    :param predicate_ids: list of predicates (as IDs) to include in results
    :param exclude_json: if true, exclude objects with JSON datatype
    :param include_all_predicates: if true, include all predicates as keys in the result dictionary,
                                   even if they have no values for the term
    :param statement: name of ontology statement table
    :param term_ids: list of term IDs to restrict results to
    :return: dict containing term ID -> predicate -> list of objects
    """
    term_objects = defaultdict(defaultdict)
    if include_all_predicates:
        # Build dict of all terms with all predicates
        tmp_ids = term_ids
        if not tmp_ids:
            # If term IDs were not included, retrieve all subjects
            # We use a "temp" variable here so that we don't have to pass all to query
            tmp_ids = [
                x["subject"]
                for x in conn.execute(f'SELECT DISTINCT subject FROM "{statement}";').fetchall()
            ]
        for term_id in tmp_ids:
            term_objects[term_id] = defaultdict(list)
            for p in predicate_ids:
                term_objects[term_id][p] = list()

    # Build a query
    query = f"""SELECT DISTINCT subject, predicate, object, datatype, annotation
                FROM "{statement}" WHERE predicate IN :predicates"""
    if term_ids:
        query += " AND subject IN :terms"

    # Add params for any where statements using user input values
    const_dict = {"predicates": predicate_ids}
    if exclude_json:
        query += " AND datatype IS NOT '_JSON'"
    query = sql_text(query).bindparams(bindparam("predicates", expanding=True))

    results = []
    if term_ids:
        # Use chunks to get around max SQL variables
        chunks = [term_ids[i : i + MAX_SQL_VARS] for i in range(0, len(term_ids), MAX_SQL_VARS)]
        for chunk in chunks:
            const_dict["terms"] = chunk
            query = query.bindparams(bindparam("terms", expanding=True))
            results.extend(conn.execute(query, const_dict).fetchall())
    else:
        results.extend(conn.execute(query, const_dict).fetchall())

    for res in results:
        s = res["subject"]
        p = res["predicate"]
        if p not in term_objects[s]:
            term_objects[s][p] = list()
        term_objects[s][p].append(
            {"object": res["object"], "datatype": res["datatype"], "annotation": res["annotation"]}
        )
    return term_objects


def get_ontology_iri(conn, statement="statement"):
    res = conn.execute(
        f"""SELECT subject FROM "{statement}"
                WHERE predicate = 'rdf:type' AND object = 'owl:Ontology'"""
    ).fetchone()
    if res:
        return res["subject"]
    return None


def get_ontology_title(
    conn: Connection, prefixes: dict, term_id: str, statement: str = "statement"
):
    # Maybe get an ontology title from dce:title property
    # People often use different prefixes for this, so check for what is used
    bases = {v: k for k, v in prefixes.items()}
    dce_prefix = bases.get("http://purl.org/dc/elements/1.1/")
    if dce_prefix:
        title_predicate = dce_prefix + ":title"
    else:
        title_predicate = "<http://purl.org/dc/elements/1.1/title>"
    res = conn.execute(
        sql_text(
            f"""SELECT object FROM "{statement}"
            WHERE subject = :ontology AND predicate = :predicate"""
        ),
        ontology=term_id,
        predicate=title_predicate,
    ).fetchone()
    if res:
        return res["object"]
    return None


def get_prefixes(conn: Connection) -> dict:
    results = conn.execute("SELECT * FROM prefix ORDER BY length(base) DESC")
    return {res["prefix"]: res["base"] for res in results}


def get_iri(prefixes: dict, term_id: str) -> str:
    """Get the IRI from a CURIE.

    :param prefixes: dict of namespace -> base
    :param term_id: term ID to get IRI of
    :return: string IRI
    """
    if term_id.startswith("<"):
        return term_id.lstrip("<").rstrip(">")
    prefix = term_id.split(":")[0]
    namespace = prefixes.get(prefix)
    if not namespace:
        raise ValueError(f"Prefix '{prefix}' is not defined in prefix table")
    local_id = term_id.split(":")[1]
    return namespace + local_id


def get_term_attributes(
    conn: Connection,
    exclude_json: bool = False,
    include_all_predicates: bool = True,
    predicates: List[str] = None,
    statement: str = "statement",
    term_ids: List[str] = None,
) -> dict:
    """Retrieve all attributes for given terms from the SQL database. If no terms are provided,
    return details for all terms in database. This is returned as a dictionary of predicate ID ->
    list of object dictionaries (object, datatype, annotation).

    :param conn: SQLAlchemy database connection
    :param exclude_json: if True, do not include objects with the _JSON datatype (anonymous)
    :param include_all_predicates: if True, include predicates in the return dicts even if they
                                   have no values for a given term.
    :param predicates: list of properties to include in export
    :param statement: name of the ontology statements table
    :param term_ids: list of terms to export (by ID or label)
    :return: string export in given format
    """
    predicate_ids = get_ids(conn, id_or_labels=predicates, id_type="predicate", statement=statement)

    # Get prefixes
    prefixes = {}
    for row in conn.execute(f"SELECT DISTINCT prefix, base FROM prefix"):
        prefixes[row["prefix"]] = row["base"]

    # Get the term details
    return get_objects(
        conn,
        predicate_ids,
        exclude_json=exclude_json,
        include_all_predicates=include_all_predicates,
        statement=statement,
        term_ids=term_ids,
    )


def get_top_entity_type(conn: Connection, term_id: str, statements="statements") -> str:
    """Get a single OWL entity type for a term. This will not include the types of named inviduals,
    rather a named individual will have the type owl:Individual."""
    query = sql_text(
        f"SELECT object FROM \"{statements}\" WHERE subject = :term_id AND predicate = 'rdf:type'"
    )
    results = list(conn.execute(query, term_id=term_id))
    if len(results) > 1:
        for res in results:
            if res["object"] in TOP_LEVELS:
                return res["object"]
        return "owl:Individual"
    elif len(results) == 1:
        entity_type = results[0]["object"]
        if entity_type == "owl:NamedIndividual":
            entity_type = "owl:Individual"
        return entity_type
    else:
        # Check if this is used as a subClass or subProperty
        entity_type = None
        query = sql_text(f'SELECT predicate FROM "{statements}" WHERE subject = :term_id')
        results = conn.execute(query, term_id=term_id)
        preds = [row["predicate"] for row in results]
        if "rdfs:subClassOf" in preds:
            return "owl:Class"
        elif "rdfs:subPropertyOf" in preds:
            return "owl:AnnotationProperty"
        if not entity_type:
            # Check if this is used as a parent property or parent class
            query = sql_text(f"SELECT predicate FROM {statements} WHERE object = :term_id")
            results = conn.execute(query, term_id=term_id)
            preds = [row["predicate"] for row in results]
            if "rdfs:subClassOf" in preds:
                return "owl:Class"
            elif "rdfs:subPropertyOf" in preds:
                return "owl:AnnotationProperty"
    return "owl:Class"
