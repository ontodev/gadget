import re

from collections import defaultdict
from hiccupy import insert_href, render
from itertools import chain
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from typing import Dict
from .render import get_html_label, pre_render_objects, render_hiccup
from .sql import (
    get_ancestor_hierarchy,
    get_descendant_hierarchy,
    get_entity_types,
    get_iri,
    get_labels,
    get_objects,
    get_ontology_iri,
    get_ontology_title,
    get_prefixes,
    TOP_LEVELS,
)
from .style import (
    BOOTSTRAP_CSS,
    BOOTSTRAP_JS,
    get_tree_js,
    JQUERY_JS,
    PLUS,
    POPPER_JS,
    SHOW_CHILDREN,
    TREE_CSS,
    TYPEAHEAD_JS,
)


def get_individuals_by_type(conn: Connection, statement: str = "statement") -> Dict[list]:
    """Return a dict of class -> list of individuals that are instances of that class.

    :param conn: database connection
    :param statement: name of ontology statement table
    :return: class -> instances
    """
    query = f"""SELECT DISTINCT subject FROM "{statement}"
            WHERE predicate = 'rdf:type' AND object = 'owl:NamedIndividual'"""
    results = conn.execute(query)
    i = [res["subject"] for res in results]
    entity_types = get_entity_types(conn, i, statement=statement)
    individuals = defaultdict(list)
    individuals["owl:Individual"] = []
    for i, et in entity_types.items():
        for e in et:
            if et == "owl:Individual" or et == "owl:NamedIndividual":
                continue
            if e not in individuals:
                individuals["owl:Individual"].append(e)
                individuals[e] = []
            individuals[e].append(i)
    return individuals


def get_sorted_predicates(
    conn: Connection, exclude_ids: list = None, statement: str = "statement"
) -> list:
    """Return a list of predicates IDs sorted by their label, optionally excluding some predicate
    IDs. If the predicate does not have a label, use the ID as the label.

    :param conn: database connection
    :param exclude_ids: list of IDs to exclude from predicates
    :param statement: name of ontology statement table
    :return: list of predicate labels in sorted order
    """
    query = f"""WITH labels AS (
        SELECT DISTINCT subject, object
        FROM "{statement}" WHERE predicate = 'rdfs:label'
    )
    SELECT DISTINCT predicate AS subject, labels.object AS object
    FROM "{statement}" s
    LEFT JOIN labels ON predicate = labels.subject WHERE predicate NOT NULL """
    if exclude_ids:
        exclude = ", ".join([f"'{x}'" for x in exclude_ids])
        query += " AND predicate NOT IN :exclude"
        query = sql_text(query).bindparams(bindparam("exclude", expanding=True))
        results = conn.execute(query, exclude=exclude)
    else:
        results = conn.execute(query)
    predicate_label_map = {res["subject"]: res["object"] or res["subject"] for res in results}

    # Return list of keys sorted by value (label)
    sorted_predicates = [
        k for k, v in sorted(predicate_label_map.items(), key=lambda x: x[1].lower())
    ]
    return sorted_predicates


def get_top_hierarchy(
    conn: Connection, entity_type: str, statement: str = "statement"
) -> Dict[list]:
    """Get the hierarchy starting at a "top" entity.

    :param conn: database connection
    :param entity_type: type of entity to get direct hierarchy under
    :param statement: name of ontology statement table
    :return: dict of parent -> list of children
    """
    top_level = entity_type
    if entity_type == "owl:Class":
        top_level = "owl:Thing"
        predicate = "rdfs:subClassOf"
    else:
        predicate = "rdfs:subPropertyOf"
        if entity_type == "owl:DatatypeProperty":
            top_level = "owl:topDataProperty"
        elif entity_type == "owl:ObjectProperty":
            top_level = "owl:topObjectProperty"
    query = sql_text(
        f"""WITH descendants(parent, child) AS (
            SELECT
                :entity_type AS parent,
                subject AS child
            FROM "{statement}"
            -- find the subjects of this type that do not have a parent
            WHERE subject IN 
                (SELECT subject FROM "{statement}" WHERE predicate = 'rdf:type' AND object = :entity_type)
                AND subject NOT IN (SELECT subject FROM "{statement}" WHERE predicate = :predicate)
                AND subject IS NOT :top_level
            UNION
            -- and the subjects that are direct children of the top-level (support for owl:Thing)
            SELECT
                :entity_type AS parent,
                subject AS child
            FROM "{statement}"
            WHERE predicate = :predicate AND object = :top_level
            UNION
            -- fill in the table with the non-blank descendants of all top level terms
            SELECT
                "{statement}".object AS parent,
                "{statement}".subject AS child
            FROM "{statement}", descendants
            WHERE descendants.child = "{statement}".object
                AND "{statement}".predicate = :predicate
                AND "{statement}".datatype = '_IRI'
        )
        SELECT * FROM descendants;"""
    )
    results = conn.execute(query, entity_type=entity_type, predicate=predicate, top_level=top_level)
    descendants = defaultdict(list)
    for res in results:
        if res["parent"] not in descendants:
            descendants[res["parent"]] = []
        descendants[res["parent"]].append(res["child"])
    return descendants


def parent2tree(treedata: dict, selected_term: str, selected_children: list, node: str) -> list:
    """Starting at a selected term, go up the hierarchy of ancestors and create a HTML "tree"
    structure as a hiccup list.

    :param treedata: data for the terms
    :param selected_term: term to create hierarchy for
    :param selected_children: children of term
    :param node: current node in hierarchy
    :return: hiccup list for hierarchy
    """
    # Remap these parents to the top tree "parent", which is the entity type
    if node == "owl:Thing":
        node = "owl:Class"
    elif node == "owl:topDataProperty":
        node = "owl:DatatypeProperty"
    elif node == "owl:topObjectProperty":
        node = "owl:ObjectProperty"

    # Create the current hierarchy list element
    if selected_children:
        cur_hierarchy = ["ul", ["li", tree_label(treedata, selected_term), selected_children]]
    else:
        cur_hierarchy = ["ul", ["li", tree_label(treedata, selected_term)]]
    if node in TOP_LEVELS:
        # Parent is top-level, nothing else to add
        return cur_hierarchy

    # Add parents to hierarhcy
    i = 0
    while node and i < 100:
        i += 1
        object_label = tree_label(treedata, node)
        parents = treedata["ancestors"].get(node)
        if not parents:
            # No parent
            cur_hierarchy = ["ul", ["li", ["a", {"resource": node}, object_label], cur_hierarchy]]
            break
        for parent in parents:
            if node == parent:
                # Parent is the same - prevent an infinite loop
                cur_hierarchy = [
                    "ul",
                    ["li", ["a", {"resource": node}, object_label], cur_hierarchy],
                ]
                node = None
                break
            if parent in TOP_LEVELS:
                href_ele = {"resource": node}
            else:
                href_ele = {
                    "about": parent,
                    "rev": "rdfs:subClassOf",
                    "resource": node,
                }
            cur_hierarchy = ["ul", ["li", ["a", href_ele, object_label], cur_hierarchy]]
            node = parent
            if node in TOP_LEVELS:
                node = None
                break
    return cur_hierarchy


def term2rdfa(
    conn: Connection,
    term_id: str,
    predicate_ids: list,
    max_children: int = 100,
    statement: str = "statement",
    title: str = None,
):
    """Render the term as HTML/RDFa hiccup list.

    :param conn: database connection
    :param term_id: term to render
    :param predicate_ids: predicates to include
    :param max_children: maximum number of children to display
    :param statement: name of ontology statement table
    :param title: ontology title
    :return: hiccup list for term tree + annotations
    """
    # Get the prefixes for converting CURIEs to IRIs
    prefixes = get_prefixes(conn)

    descendants = {}
    ancestors = {}
    ontology_title = None
    if term_id in TOP_LEVELS and term_id not in ["owl:Ontology", "owl:Individual", "rdfs:Datatype"]:
        descendants = get_top_hierarchy(conn, term_id, statement=statement)
    elif term_id == "owl:Individual":
        descendants = get_individuals_by_type(conn, statement=statement)
    elif term_id == "rdfs:Datatype":
        pass
    elif term_id == "owl:Ontology":
        term_id = get_ontology_iri(conn, statement=statement)
        ontology_title = get_ontology_title(conn, prefixes, term_id, statement=statement)
    else:
        descendants = get_descendant_hierarchy(conn, [term_id], statement=statement)
        ancestors = get_ancestor_hierarchy(conn, [term_id], statement=statement)

    # Get the attributes (annotations, logic) of our term
    term_objects = get_objects(
        conn, predicate_ids, include_all_predicates=False, statement=statement, term_ids=[term_id]
    )
    pre_render, term_ids = pre_render_objects(term_objects)

    # Add all the IDs of terms we care about
    term_ids.add(term_id)
    term_ids.update(predicate_ids)
    term_ids.update(chain.from_iterable(ancestors.values()))
    # Only add the direct children
    term_ids.update(descendants.get(term_id, []))
    term_ids = list(term_ids)

    labels = get_labels(conn, term_ids, statement=statement)
    if ontology_title:
        # This is the owl:Ontology node and we have a title, which will be used as the "label"
        labels[term_id] = ontology_title
    entity_types = get_entity_types(conn, term_ids, statement=statement)

    # Get obsolete terms
    query = sql_text(
        f"""SELECT subject FROM "{statement}"
        WHERE subject IN :term_ids
          AND predicate = "owl:deprecated"
          AND LOWER(object) = "true";"""
    ).bindparams(bindparam("term_ids", expanding=True))
    obsolete = [res["subject"] for res in conn.execute(query, term_ids=list(term_ids)).fetchall()]

    treedata = {
        "ancestors": ancestors,
        "descendants": descendants,
        "entity_types": entity_types,
        "labels": labels,
        "obsolete": obsolete,
    }
    rdfa_tree = term2tree(treedata, term_id, max_children=max_children)

    # Build the hiccup list for this term
    if term_id in TOP_LEVELS and term_id != "owl:Ontology":
        items = [
            "ul",
            {"id": "annotations", "class": "col-md"},
            ["p", {"class": "lead"}, "Hello! This is an ontology browser."],
            [
                "p",
                "An ",
                [
                    "a",
                    {"href": "https://en.wikipedia.org/wiki/Ontology_(information_science)"},
                    "ontology",
                ],
                " is a terminology system designed for both humans and machines to read. Click the",
                " links on the left to browse the hierarchy of terms. Terms have parent terms, ",
                "child terms, annotations, and ",
                [
                    "a",
                    {"href": "https://en.wikipedia.org/wiki/Web_Ontology_Language"},
                    "logical axioms",
                ],
                ". The page for each term is also machine-readable using ",
                ["a", {"href": "https://en.wikipedia.org/wiki/RDFa"}, "RDFa"],
                ".",
            ],
        ]
        if title:
            term = [
                "div",
                ["div", {"class": "row"}, ["h2", title]],
                ["div", {"class": "row", "style": "padding-top: 10px;"}, rdfa_tree, items],
            ]
        else:
            term = [
                "div",
                ["div", {"class": "row", "style": "padding-top: 10px;"}, rdfa_tree, items],
            ]
    else:
        term_iri = get_iri(prefixes, term_id)
        if pre_render:
            # Order the pre-render by predicates as determined by predicate_ids
            pre_render = sorted(
                pre_render[term_id].items(), key=lambda x: predicate_ids.index(x[0])
            )
            object_hiccup = {
                p: render_hiccup(
                    p, o, labels, entity_types, include_annotations=True, single_item_list=True
                )
                for p, o in pre_render
            }
            attrs = ["ul", {"id": "annotations", "style": "margin-left: -1rem;"}]
            for predicate, objs in object_hiccup.items():
                pred_label = get_html_label(predicate, labels)
                attrs.append(["li", pred_label, objs])
            attrs = [
                "div",
                {"class": "col-md"},
                ["div", {"class": "row"}, ["h4", {"id": "label"}, labels.get(term_id, term_id)]],
                attrs,
            ]
            term = [
                "div",
                {"resource": term_id},
                ["div", {"class": "row"}, ["a", {"href": term_iri}, term_iri]],
                ["div", {"class": "row", "style": "padding-top: 10px;"}, rdfa_tree, attrs],
            ]
        else:
            term = [
                "div",
                {"resource": term_id},
                ["div", {"class": "row"}, ["a", {"href": term_iri}, term_iri]],
                ["div", {"class": "row", "style": "padding-top: 10px;"}, rdfa_tree],
            ]
    return term


def term2tree(treedata: dict, term_id: str, max_children: int = 100) -> list:
    """Render the term as a hiccup tree with ancestors and direct children.

    :param treedata: data for this term
    :param term_id: "selected" term
    :param max_children: maximum number of children to display under selected term
    :return: hiccup list rendering of hierarchy
    """
    # Sort children based on label - obsolete last
    children = [
        x for x in treedata["descendants"].get(term_id, []) if x not in treedata["obsolete"]
    ]
    obsolete_children = [
        x for x in treedata["descendants"].get(term_id, []) if x in treedata["obsolete"]
    ]
    children.sort(key=lambda x: treedata["labels"].get(x, x))
    obsolete_children.sort(key=lambda x: treedata["labels"].get(x, x))
    children.extend(obsolete_children)

    entity_type = None
    if term_id in TOP_LEVELS:
        entity_type = term_id
    else:
        for et in treedata["entity_types"].get(term_id, []):
            if et in TOP_LEVELS:
                entity_type = et
                break
    if not entity_type or entity_type == "owl:Individual":
        predicate = "rdf:type"
    elif entity_type == "owl:Class":
        predicate = "rdfs:subClassOf"
    else:
        predicate = "rdfs:subPropertyOf"

    if len(children) == 0:
        children_list = []
    else:
        children_list = []
        for child in children:
            object_label = tree_label(treedata, child)
            o = ["a", {"rev": predicate, "resource": child}, object_label]
            # Check for children of the child and add a plus next to label if so
            if treedata["descendants"].get(child):
                o.append(PLUS)
            attrs = {}
            if len(children_list) >= max_children:
                attrs["style"] = "display: none"
            children_list.append(["li", attrs, o])

        if len(children) >= max_children:
            children_list.append(
                [
                    "li",
                    {"id": "more"},
                    [
                        "a",
                        {"href": f"javascript:show_children()"},
                        f"Click to show all {len(children)} ...",
                    ],
                ]
            )
        children_list = ["ul", {"id": "children"}] + children_list

    if term_id in TOP_LEVELS:
        term_label = TOP_LEVELS[term_id]
    else:
        term_label = tree_label(treedata, term_id)

    # Get the parents of our target term and create the tree hierarchy
    parents = treedata["ancestors"].get(term_id)
    if parents:
        hierarchy = ["ul"]
        for p in parents:
            hierarchy.append(parent2tree(treedata, term_id, children_list.copy(), p))
    else:
        if children_list:
            hierarchy = ["ul", ["li", term_label, children_list]]
        else:
            hierarchy = ["ul", ["li", term_label]]

    hierarchies = ["ul", {"id": f"hierarchy", "class": "hierarchy multiple-children col-md"}]
    for t, object_label in TOP_LEVELS.items():
        o = ["a", {"resource": t}, object_label]
        if t == entity_type:
            if term_id == entity_type:
                hierarchies.append(hierarchy)
            else:
                hierarchies.append(["ul", ["li", o, hierarchy]])
            continue
        hierarchies.append(["ul", ["li", o]])
    return hierarchies


def tree(
    conn: Connection,
    term_id: str = None,
    db_query_param: str = "db",
    href: str = "?id={curie}",
    include_search: bool = True,
    max_children: int = 100,
    predicate_ids: list = None,
    standalone: bool = True,
    statement: str = "statement",
    title: str = None,
) -> str:
    """Render the given term as an HTML/RDFa tree + details.

    :param conn: database connection
    :param term_id: term to display
    :param db_query_param:
    :param href: URL pattern for links to other terms, where {curie} is replaced with the term ID
    :param include_search: if True, include a search bar at the top of the page
    :param max_children: maximum number of children to display under term
    :param predicate_ids: predicates to include in term details on right side of page
    :param standalone: if True, include HTML headers
    :param statement: name of ontology statement table
    :param title: ontology title to be displayed on page
    :return: HTML/RDFa string
    """
    body = []
    if not term_id:
        t = term2rdfa(
            conn, "owl:Class", [], title=title, max_children=max_children, statement=statement,
        )
        body.append(t)
    else:
        # Maybe find a * in the IDs that represents all remaining predicates
        if predicate_ids and "*" in predicate_ids:
            before = []
            after = []
            found = False
            for pred in predicate_ids:
                if pred == "*":
                    found = True
                    continue
                if not found:
                    before.append(pred)
                else:
                    after.append(pred)
            predicate_ids_split = [before, after]

            # If some IDs were provided with *, add the remaining predicates
            # These properties go in between the before & after defined in the split
            rem_predicate_ids = get_sorted_predicates(
                conn, exclude_ids=predicate_ids, statement=statement
            )

            # Separate before & after with the remaining properties
            predicate_ids = predicate_ids_split[0]
            predicate_ids.extend(rem_predicate_ids)
            predicate_ids.extend(predicate_ids_split[1])
        elif not predicate_ids:
            predicate_ids = get_sorted_predicates(conn, statement=statement)
        t = term2rdfa(
            conn,
            term_id,
            predicate_ids,
            title=title,
            max_children=max_children,
            statement=statement,
        )
        body.append(t)

    # Create the prefix element
    results = conn.execute("SELECT * FROM prefix ORDER BY length(base) DESC")
    prefixes = {res["prefix"]: res["base"] for res in results}
    pref_strs = []
    for prefix, base in prefixes.items():
        pref_strs.append(f"{prefix}: {base}")
    pref_str = "\n".join(pref_strs)

    body_wrapper = ["div", {"id": f"{statement}-tree", "class": f"gadgetTree", "prefix": pref_str}]
    if include_search:
        body_wrapper.append(
            [
                "div",
                {"class": "form-row mt-2 mb-2"},
                [
                    "input",
                    {
                        "id": f"statements-typeahead",
                        "class": "typeahead form-control",
                        "type": "text",
                        "value": "",
                        "placeholder": "Search",
                    },
                ],
            ]
        )
    body = body_wrapper + body

    # JQuery
    if standalone:
        body.append(
            [
                "cmi_pb_script",
                {
                    "src": JQUERY_JS,
                    "integrity": "sha256-9/aliU8dGd2tb6OSsuzixeV4y/faTqgFtohetphbbj0=",
                    "crossorigin": "anonymous",
                },
            ]
        )

        if include_search:
            # Add JS imports for running search
            body.append(["cmi_pb_script", {"type": "text/javascript", "src": POPPER_JS}])
            body.append(["cmi_pb_script", {"type": "text/javascript", "src": BOOTSTRAP_JS}])
            body.append(["cmi_pb_script", {"type": "text/javascript", "src": TYPEAHEAD_JS}])

        # Custom JS for show more children
        js = SHOW_CHILDREN

        # Custom JS for search bar using Typeahead
        if include_search:
            # Built the href to return when you select a term
            href_split = href.split("{curie}")
            before = href_split[0]
            after = href_split[1]
            js_funct = f'str.push("{before}" + encodeURIComponent(obj[p]) + "{after}");'

            # Build the href to return names JSON
            if f"{db_query_param}=" in href:
                db = re.search(f"{db_query_param}=([^&]+)&?", href).group(1)
                remote = f"'?{db_query_param}={db}&text=%QUERY&format=json"
            else:
                remote = "'?text=%QUERY&format=json'"
            js += get_tree_js(remote, js_funct)

        body.append(["cmi_pb_script", {"type": "text/javascript"}, js])

        # HTML Headers & CSS
        head = [
            "head",
            ["meta", {"charset": "utf-8"}],
            [
                "meta",
                {
                    "name": "viewport",
                    "content": "width=device-width, initial-scale=1, shrink-to-fit=no",
                },
            ],
            ["link", {"rel": "stylesheet", "href": BOOTSTRAP_CSS, "crossorigin": "anonymous"}],
            ["link", {"rel": "stylesheet", "href": "../style.css"}],
        ]
        if title:
            head.append(["title", title])
        head.append(["style", TREE_CSS])
        body = ["body", {"class": "container"}, body]
        html = ["html", head, body]
    else:
        html = body

    html = insert_href(html, href=href)
    return render(html)


def tree_label(treedata: dict, term_id: str) -> list:
    """Retrieve the hiccup-style vector label of a term.

    :param treedata: data for relevant terms
    :param term_id: term to get label of
    :return: hiccup list for term label
    """
    label = treedata["labels"].get(term_id, term_id)
    if term_id in treedata["obsolete"]:
        return ["s", label]
    return label
