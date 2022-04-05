import json
import wiring_rs

from collections import defaultdict
from html import escape as html_escape
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from typing import Dict, Optional, Tuple, List

MAX_SQL_VARS = 999
TOP_LEVELS = {
    "ontology": "Ontology",
    "owl:Class": "Class",
    "owl:AnnotationProperty": "Annotation Property",
    "owl:DataProperty": "Data Property",
    "owl:ObjectProperty": "Object Property",
    "owl:Individual": "Individual",
    "rdfs:Datatype": "Datatype",
}


# ----- QUERYING -----


def get_ancestor_hierarchy(conn: Connection, term_id: str, statement="statement",) -> dict:
    """Return a dict of child -> list of parents for the full ancestor lineage of the given term.

    :param conn: database connection to query
    :param term_id: term to get ancestors of
    :param statement: name of the ontology statement table
    :return: dict of child -> set of parents
    """
    query = sql_text(f"SELECT child, parent FROM {statement}_ancestor_view WHERE target = :target")
    results = conn.execute(query, term_id=term_id).fetchall()
    ancestors = defaultdict(set)
    for res in results:
        if res["child"] not in ancestors:
            ancestors[res["child"]] = set()
        ancestors[res["child"]].add(res["parent"])
    return ancestors


def get_descendant_hierarchy(conn: Connection, term_id: str, statement: str = "statement"):
    query = sql_text(
        f"SELECT DISTINCT child, parent FROM {statement}_descendant_view WHERE target = :target"
    )
    results = conn.execute(query, target=term_id)
    descendants = defaultdict(set)
    for res in results:
        if res["parent"] not in descendants:
            descendants[res["parent"]] = set()
        descendants[res["parent"]].add(res["child"])
    return descendants


def get_descendants(conn: Connection, term_id: str, statement: str = "statements") -> set:
    """Return a set of descendants (in no order) for a given term ID."""
    query = sql_text(
        f"SELECT DISTINCT child FROM {statement}_descendant_view WHERE target = :target"
    )
    results = conn.execute(query, target=term_id)
    return set([x["child"] for x in results])


def get_entity_types(conn: Connection, term_ids: list, statement="statement") -> Dict[str, set]:
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


def get_ids(
    conn: Connection, id_or_labels: list = None, id_type="subject", statement: str = "statement",
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


def get_term_attributes(
    conn: Connection,
    exclude_json: bool = False,
    include_all_predicates: bool = True,
    predicates: list = None,
    statement: str = "statement",
    term_ids: Optional[list] = None,
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
    if term_ids:
        # Use list of terms (IDs or labels) to get a list of term IDs
        term_ids = get_ids(conn, id_or_labels=term_ids, statement=statement)
    else:
        # No term IDs, we will return details for all subjects in the database
        term_ids = None

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


def get_objects(
    conn: Connection,
    predicate_ids: list,
    exclude_json: bool = False,
    include_all_predicates: bool = True,
    statement: str = "statement",
    term_ids: list = None,
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

    results = []
    if term_ids:
        # Use chunks to get around max SQL variables
        chunks = [term_ids[i : i + MAX_SQL_VARS] for i in range(0, len(term_ids), MAX_SQL_VARS)]
        for chunk in chunks:
            query = f"""SELECT DISTINCT subject, predicate, object, datatype, annotation
                    FROM "{statement}" WHERE subject IN :terms AND predicate IN :predicates"""
            if exclude_json:
                query += " AND datatype IS NOT '_JSON'"
            query = sql_text(query).bindparams(
                bindparam("terms", expanding=True), bindparam("predicates", expanding=True)
            )
            results.extend(
                conn.execute(query, {"terms": chunk, "predicates": predicate_ids}).fetchall()
            )
    else:
        query = f"""SELECT DISTINCT subject, predicate, object, datatype, annotation
                FROM "{statement}" WHERE predicate IN :predicates"""
        if exclude_json:
            query += " AND datatype IS NOT '_JSON'"
        query = sql_text(query).bindparams(bindparam("predicates", expanding=True))
        results.extend(conn.execute(query, {"predicates": predicate_ids}).fetchall())

    for res in results:
        s = res["subject"]
        p = res["predicate"]
        if p not in term_objects[s]:
            term_objects[s][p] = list()
        term_objects[s][p].append(
            {"object": res["object"], "datatype": res["datatype"], "annotation": res["annotation"]}
        )
    return term_objects


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


def get_labels(conn, term_ids: list, statement="statement"):
    """Return a dictionary of term ID -> label.

    :param conn: database connection to query
    :param term_ids: list of term IDs to get labels for
    :param statement: name of ontology statement table
    :return:
    """
    labels = {}
    query = sql_text(
        f"""SELECT subject, object FROM "{statement}"
            WHERE subject IN :ids AND predicate = 'rdfs:label' AND object IS NOT NULL"""
    ).bindparams(bindparam("ids", expanding=True))
    results = conn.execute(query, {"ids": term_ids})
    for res in results:
        labels[res["subject"]] = res["object"]
    return labels


# ----- RENDERING -----


def get_html_label(iri: str, labels: dict, predicate: str = None) -> list:
    """Create a hiccup-style RDFa list for a term.

    :param iri: term IRI (to use as RDFa 'resource')
    :param labels: dict of term ID -> label
    :param predicate: optional predicate ID to include as RDFa 'property'
    :return: hiccup-style list
    """
    iri_label = iri
    if iri.startswith("<") and iri.endswith(">"):
        iri_label = iri[1:-1]
    html_label = ["a"]
    if predicate:
        html_label.append({"property": predicate, "resource": iri})
    else:
        html_label.append({"resource": iri})
    html_label.append(labels.get(iri, html_escape(iri_label)))
    return html_label


def object_to_hiccup(
    predicate, obj, labels, entity_types, as_list=False, include_annotations=False
) -> list:
    """Render an object as a hiccup-style list.
    
    :param predicate: the predicate used with this object (for RDFa)
    :param obj: object to render (dict containing object, annotation, and datatype)
    :param labels: dict of term ID -> label
    :param entity_types: dict of term ID -> OWL entity type
    :param as_list: if true, render the object as a list item (li)
    :param include_annotations: if true, include any 'annotation' objects from obj dict
    :return: hiccup-style list to render as RDFa
    """
    if as_list:
        ele = ["li"]
    else:
        ele = ["p"]
    dt = obj["datatype"]
    if dt.lower() == "_json":
        # TODO: change to RDFa rendering here when ready (returns hiccup)
        typed = wiring_rs.ofn_typing(json.dumps(obj["object"]), entity_types)
        labeled = wiring_rs.ofn_labeling(typed, labels)
        ele.append(json.loads(wiring_rs.object_2_rdfa(labeled)))
    elif dt.lower() == "_iri":
        obj_label = get_html_label(obj["object"], labels, predicate=predicate)
        ele.append(obj_label)
    else:
        if dt.startswith("@"):
            dt_display = dt
        else:
            dt_display = ["a", {"resource": dt}, dt]
        ele.append(obj["object"])
        ele.append(["sup", {"class": "text-black-50"}, dt_display])
    if obj["annotation"] and include_annotations:
        ann_ele = ["ul"]
        for ann_predicate, ann_objects in obj["annotation"].items():
            pred_ele = ["ul"]
            for ao in ann_objects:
                # TODO: support _json?
                if ao["datatype"].lower() == "_iri":
                    ao_label = get_html_label(ao["object"], labels, predicate=ann_predicate)
                    pred_ele.append(["li", ["small", ao_label]])
                else:
                    # TODO: render datatype/lang tags
                    pred_ele.append(["li", ["small", html_escape(ao["object"])]])
            ann_pred_label = get_html_label(ann_predicate, labels)
            ann_ele.append(["li", ["small", ann_pred_label], pred_ele])
        ele.append(ann_ele)
    return ele


def object_to_str(obj: dict, labels: dict, entity_types: dict) -> str:
    """Render an object as a string. JSON objects are rendered as Manchester.
    
    :param obj: object to render (dict containing object and datatype)
    :param labels: dict of term ID -> label
    :param entity_types: dict of term ID -> OWL entity type
    :return: string rendering of object
    """
    dt = obj["datatype"]
    if dt.lower() == "_json":
        typed = wiring_rs.ofn_typing(json.dumps(obj["object"]), entity_types)
        labeled = wiring_rs.ofn_labeling(typed, labels)
        return wiring_rs.ofn_2_man(labeled)
    elif dt.lower() == "_iri":
        return labels.get(obj["object"], obj["object"])
    else:
        # TODO: datatypes?
        return obj["object"]


def objects_to_hiccup(
    conn, data, include_annotations=False, single_item_list=False, statement="statement"
) -> dict:
    """Render the objects dictionary of term attributes (term ID -> predicate -> objects)
    as RDFa hiccup-style lists.

    :param conn: database connection to query
    :param data: query data in format term ID -> predicate -> objects
    :param include_annotations: if True, include axiom annotations as sub-lists
    :param statement: name of ontology statement table
    :return: dict of term ID -> predicate -> hiccup-style objects
    """
    # First pass to render as OFN list and get all the needed term IDs for labeling
    pre_render, object_ids = pre_render_objects(data)

    # Get labels and entity types for Manchester rendering
    object_ids = list(object_ids)
    labels = get_labels(conn, object_ids, statement=statement)
    entity_types = get_entity_types(conn, object_ids, statement=statement)

    # Second pass to render the OFN as Manchester with labels
    return pre_render_to_hiccup(
        pre_render,
        labels,
        entity_types,
        include_annotations=include_annotations,
        single_item_list=single_item_list,
    )


def pre_render_to_hiccup(
    pre_render, labels, entity_types, include_annotations=False, single_item_list=False
):
    """Parse the objects of a pre-rendered dictionary of terms to hiccup-style lists to be rendered
    as HTML.

    :param pre_render: 
    :param labels:
    :param entity_types:
    :param include_annotations:
    :param single_item_list:
    :return:
    """
    rendered = {}
    for term_id, predicate_objects in pre_render.items():
        rendered_term = defaultdict()
        for predicate, objs in predicate_objects.items():
            if len(objs) > 1 or single_item_list:
                lst = ["ul", {"class": "annotations"}]
                lst.extend(
                    [
                        object_to_hiccup(
                            predicate,
                            x,
                            labels,
                            entity_types,
                            as_list=True,
                            include_annotations=include_annotations,
                        )
                        for x in objs
                    ]
                )
                rendered_term[predicate] = lst
            elif len(objs) == 1:
                rendered_term[predicate] = object_to_hiccup(
                    predicate,
                    objs[0],
                    labels,
                    entity_types,
                    include_annotations=include_annotations,
                )
            else:
                rendered_term[predicate] = []
        # term ID -> predicate IDs -> hiccup lists
        rendered[term_id] = rendered_term
    return rendered


def pre_render_objects(data: dict) -> Tuple[dict, set]:
    """Perform a first-pass over data to render JSON objects as OFN lists and to collect all term
    IDs used as predicates or objects.

    :param data: data from query
    :return: pre-rendered dictionary, list of term IDs
    """
    pre_render = {}
    object_ids = set()
    for term_id, predicate_objects in data.items():
        object_ids.add(term_id)
        pre_render_term = defaultdict()
        for predicate, objs in predicate_objects.items():
            object_ids.add(predicate)
            pre_render_po = []
            for obj in objs:
                annotation = obj["annotation"]
                pre_render_annotation = defaultdict(list)
                if annotation:
                    # TODO: do we need to support more levels of annotations?
                    annotation = json.loads(obj["annotation"])
                    for ann_predicate, anns in annotation.items():
                        object_ids.add(ann_predicate)
                        pre_render_annotation[ann_predicate] = list()
                        for ann in anns:
                            # TODO: support _json?
                            if ann["datatype"].lower() == "_iri":
                                object_ids.add(ann["object"])
                            pre_render_annotation[ann_predicate].append(
                                {"object": ann["object"], "datatype": ann["datatype"]}
                            )

                if obj["datatype"].lower() == "_json":
                    ofn = wiring_rs.object_2_ofn(obj["object"])
                    pre_render_po.append(
                        {
                            "object": json.loads(ofn),
                            "datatype": obj["datatype"],
                            "annotation": pre_render_annotation,
                        }
                    )
                    object_ids.update(wiring_rs.get_signature(ofn))
                elif obj["datatype"].lower() == "_iri":
                    pre_render_po.append(
                        {
                            "object": obj["object"],
                            "datatype": obj["datatype"],
                            "annotation": pre_render_annotation,
                        }
                    )
                    object_ids.add(obj["object"])
                else:
                    pre_render_po.append(
                        {
                            "object": obj["object"],
                            "datatype": obj["datatype"],
                            "annotation": pre_render_annotation,
                        }
                    )
            pre_render_term[predicate] = pre_render_po
        pre_render[term_id] = pre_render_term
    return pre_render, object_ids


def terms_to_rows(
    conn: Connection, data: dict, sep: str = "|", statement: str = "statement"
) -> List[dict]:
    """Transform data from SQLite database to a list of dicts suitable for DictWriters.

    :param conn: database connection to query for labels and entity types
    :param data: data from query
    :param sep: separator for multiple values in a single cell
    :param statement: name of ontology statement table
    :return: list of dicts to write to table using csv.DictWriter
    """
    # First pass to render as OFN list and get all the needed term IDs for labeling
    pre_render, object_ids = pre_render_objects(data)

    # Get labels and entity types for Manchester rendering
    object_ids = list(object_ids)
    labels = get_labels(conn, object_ids, statement=statement)
    entity_types = get_entity_types(conn, object_ids, statement=statement)

    # Second pass to render the OFN as Manchester with labels
    rendered = []
    for term_id, predicate_objects in pre_render.items():
        rendered_term = {"ID": term_id}
        for predicate, objs in predicate_objects.items():
            pred_label = labels.get(predicate, predicate)
            strs = [object_to_str(o, labels, entity_types) for o in objs]
            rendered_term[pred_label] = sep.join(strs)
        rendered.append(rendered_term)
    return rendered


# ----- SEARCH -----


def search(
    conn: Connection,
    limit: Optional[int] = None,
    search_text: str = None,
    statement: str = "statement",
    term_ids: list = None,
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
        query += " AND subject IN :term_ids ORDER BY LENGTH(label)"
        if limit:
            query += f" LIMIT {limit}"
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
        query += " ORDER BY LENGTH(label)"
        if limit:
            query += f" LIMIT {limit}"
        results = conn.execute(sql_text(query), search_text=f"%%{search_text.lower()}%%").fetchall()
    return [
        {"id": res["subject"], "label": res["label"], "order": i}
        for i, res in enumerate(results, 1)
    ]
