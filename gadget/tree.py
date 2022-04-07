import re

from collections import defaultdict
from hiccupy import insert_href, render
from itertools import chain
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.sql.expression import text as sql_text
from .render import get_html_label, pre_render_objects, render_hiccup
from .sql import (
    get_ancestor_hierarchy,
    get_descendant_hierarchy,
    get_entity_types,
    get_iri,
    get_labels,
    get_objects,
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


def get_individuals_by_type(conn: Connection, statement: str = "statement"):
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
    IDs. If the predicate does not have a label, use the ID as the label."""
    query = f"""WITH labels AS (
        SELECT DISTINCT subject, object
        FROM "{statement}" WHERE predicate = 'rdfs:label'
    )
    SELECT DISTINCT predicate AS subject, labels.object AS object
    FROM "{statement}" s
    LEFT JOIN labels ON predicate = labels.subject"""
    if exclude_ids:
        exclude = ", ".join([f"'{x}'" for x in exclude_ids])
        query += " WHERE predicate NOT IN :exclude"
        query = sql_text(query).bindparams(bindparam("exclude", expanding=True))
        results = conn.execute(query, exclude=exclude)
    else:
        results = conn.execute(query)
    predicate_label_map = {res["subject"]: res["object"] or res["subject"] for res in results}

    # Return list of keys sorted by value (label)
    sorted_predicates = [k for k, v in sorted(predicate_label_map.items(), key=lambda x: x[1].lower())]
    if "dct:title" in sorted_predicates:
        # put ontology title at the start of list
        sorted_predicates.remove("dct:title")
        sorted_predicates.insert(0, "dct:title")
    return sorted_predicates


def get_top_hierarchy(conn: Connection, entity_type: str, statement: str = "statement"):
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


def parent2tree(treedata: dict, selected_term: str, selected_children: list, node: str):
    if selected_children:
        cur_hierarchy = ["ul", ["li", tree_label(treedata, selected_term), selected_children]]
    else:
        cur_hierarchy = ["ul", ["li", tree_label(treedata, selected_term)]]
    if node in TOP_LEVELS:
        # Parent is top-level, nothing to add
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
        # TODO: multiple parents?
        for parent in parents:
            if parent == "owl:Thing":
                parent = "owl:Class"
            if node == parent:
                # Parent is the same
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
    title: str = "Ontology Browser",
):
    # Get the prefixes for converting CURIEs to IRIs
    results = conn.execute("SELECT * FROM prefix ORDER BY length(base) DESC")
    prefixes = {res["prefix"]: res["base"] for res in results}

    descendants = {}
    ancestors = {}
    if term_id in TOP_LEVELS and term_id not in ["owl:Ontology", "owl:Individual", "rdfs:Datatype"]:
        descendants = get_top_hierarchy(conn, term_id, statement=statement)
    elif term_id == "owl:Individual":
        descendants = get_individuals_by_type(conn, statement=statement)
    elif term_id == "rdfs:Datatype":
        pass
    elif term_id == "owl:Ontology":
        res = conn.execute(
            f"""SELECT subject FROM "{statement}"
                                WHERE predicate = 'rdf:type' AND object = 'owl:Ontology'"""
        ).fetchone()
        if res:
            term_id = res["subject"]
    else:
        descendants = get_descendant_hierarchy(conn, term_id, statement=statement)
        ancestors = get_ancestor_hierarchy(conn, term_id, statement=statement, sub_class=True)

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
    if term_id in TOP_LEVELS and term_id != 'owl:Ontology':
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
        term = ["div", ["div", {"class": "row"}, ["h2", title]],
                ["div", {"class": "row", "style": "padding-top: 10px;"}, rdfa_tree, items]]
    else:
        if pre_render:
            object_hiccup = render_hiccup(
                pre_render, labels, entity_types, include_annotations=True, single_item_list=True
            )[term_id]
            attrs = ["ul", {"id": "annotations", "class": "col-md"}]
            for predicate, objs in object_hiccup.items():
                if predicate == "rdfs:label":
                    # Label is already on the left side
                    continue
                pred_label = get_html_label(predicate, labels)
                attrs.append(["li", pred_label, objs])
            term_iri = get_iri(prefixes, term_id)
            term = [
                "div",
                {"resource": term_id},
                ["div", {"class": "row"}, ["h2", labels.get(term_id, term_id)]],
                ["div", {"class": "row"}, ["a", {"href": term_iri}, term_iri]],
                ["div", {"class": "row", "style": "padding-top: 10px;"}, rdfa_tree, attrs],
            ]
        else:
            term_iri = get_iri(prefixes, term_id)
            term = [
                "div",
                {"resource": term_id},
                ["div", {"class": "row"}, ["h2", labels.get(term_id, term_id)]],
                ["div", {"class": "row"}, ["a", {"href": term_iri}, term_iri]],
                ["div", {"class": "row", "style": "padding-top: 10px;"}, rdfa_tree],
            ]
    return term


def term2tree(treedata: dict, term_id: str, max_children: int = 100):
    # Sort children based on label - obsolete last
    children = [x for x in treedata["descendants"].get(term_id, []) if x not in treedata["obsolete"]]
    obsolete_children = [x for x in treedata["descendants"].get(term_id, []) if x in treedata["obsolete"]]
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
    title: str = "Ontology Browser",
) -> str:
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

    body_wrapper = ["div", {"id": "gizmosTree", "prefix": pref_str}]
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
            ["title", title],
            ["style", TREE_CSS],
        ]
        body = ["body", {"class": "container"}, body]
        html = ["html", head, body]
    else:
        html = body

    html = insert_href(html, href=href)
    return render(html)


def tree_label(treedata: dict, s: str) -> list:
    """Retrieve the hiccup-style vector label of a term."""
    label = treedata["labels"].get(s, s)
    if s in treedata["obsolete"]:
        return ["s", label]
    return label
