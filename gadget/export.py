import csv
import hiccupy
import json
import logging
import re
import sys

from argparse import ArgumentParser
from collections import defaultdict
from io import StringIO
from sqlalchemy.engine.base import Connection
from .cli_helpers import get_connection, get_terms
from .render import clean_object, object2str, pre_render_objects, render_hiccup
from .sql import get_entity_types, get_ids, get_labels, get_objects, get_prefixes


def main():
    p = ArgumentParser()

    # Global options
    p.add_argument(
        "-d", "--database", required=True, help="Database file (.db) or configuration (.ini)"
    )
    p.add_argument(
        "-S",
        "--statement",
        help="Name of the ontology table (default: statement)",
        default="statement",
    )
    p.add_argument("-t", "--term", action="append", help="CURIE or label of term to extract")
    p.add_argument(
        "-T", "--terms", help="File containing CURIES or labels of terms to extract",
    )
    p.add_argument(
        "-p", "--predicate", action="append", help="CURIE or label of predicate to include",
    )
    p.add_argument(
        "-P", "--predicates", help="File containing CURIEs or labels of predicates to include",
    )

    # Export options
    p.add_argument(
        "-a",
        "--include-annotations",
        action="store_true",
        help="Include annotations as additional columns when present",
    )
    p.add_argument(
        "-c",
        "--contents-only",
        action="store_true",
        help="If provided with HTML format, render HTML without roots",
    )
    p.add_argument("-f", "--format", help="Output format (tsv, csv, html)", default="tsv")
    p.add_argument("-s", "--split", help="Character to split multiple values on", default="|")
    p.add_argument(
        "-V",
        "--values",
        help="Default value format for cell values (default: label)",
        default="LABEL",
    )
    args = p.parse_args()

    terms = get_terms(args.term, args.terms)

    # Get predicates & maybe value formats for each predicate
    # - if a value format is not provided in the predicate, use the --values option
    predicates = get_terms(args.predicate, args.predicates)
    value_formats = {}
    predicates_clean = []
    for p in predicates:
        m = re.match(r"(.+) \[(.+)]$", p)
        if m:
            p = m.group(1)
            value_formats[p] = m.group(2)
        predicates_clean.append(p)

    # Run export
    conn = get_connection(args.database)
    sys.stdout.write(
        export(
            conn,
            default_value_format=args.values.upper(),
            fmt=args.format,
            include_annotations=args.include_annotations,
            predicates=predicates_clean,
            sep=args.split,
            standalone=not args.contents_only,
            statement=args.statement,
            terms=terms,
            value_formats=value_formats,
        )
    )


def dicts2rdfa(rendered_data: list, headers: list, prefixes: dict, standalone: bool = True) -> str:
    """Transform data with pre-rendered values to RDFa.

    :param rendered_data: list of dicts with hiccup lists as predicate values
    :param headers: list of headers for output
    :param prefixes: dict of prefix -> base
    :param standalone: if True, include HTML root & headers
    :return: rendered HTML/RDFa string
    """
    thead = [["th", h] for h in headers]
    thead.insert(0, "tr")
    thead = ["thead", thead]
    tbody = ["tbody"]
    for itm in rendered_data:
        tr = ["tr"]
        for h in headers:
            v = itm.get(h)
            if not v:
                tr.append(["td"])
            else:
                tr.append(["td", v])
        tbody.append(tr)

    # Create the prefix element
    pref_strs = []
    for prefix, base in prefixes.items():
        pref_strs.append(f"{prefix}: {base}")
    pref_str = "\n".join(pref_strs)

    table = ["table", {"class": "table table-striped", "prefix": pref_str}, thead, tbody]
    if standalone:
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
            [
                "link",
                {
                    "rel": "stylesheet",
                    "href": "https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css",
                    "crossorigin": "anonymous",
                },
            ],
        ]
        html = ["html", head, ["body", ["div", {"class": "container"}, table]]]
    else:
        html = table
    return hiccupy.render(html)


def dicts2tsv(rendered_data: list, headers: list, delimiter: str = "\t",) -> str:
    """Transform data with pre-rendered values to a list of dicts suitable for DictWriters.

    :param rendered_data: list of dicts to write to DictWriter
    :param headers: list of headers for output
    :param delimiter: character to separate cells (default '\t' for TSV)
    :return: string table (TSV or CSV) output
    """
    output = StringIO()
    writer = csv.DictWriter(output, delimiter=delimiter, fieldnames=headers, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rendered_data)
    return output.getvalue()


def export(
    conn: Connection,
    default_value_format: str = "LABEL",
    fmt: str = "tsv",
    include_annotations: bool = False,
    predicates: list = None,
    sep: str = "|",
    standalone: bool = True,
    statement: str = "statement",
    terms: list = None,
    value_formats: dict = None,
) -> str:
    """Export details about ontology terms in the specified format.

    :param conn: database connection
    :param default_value_format: format to render ontology terms (CURIE, IRI, or LABEL) in cells
    :param fmt: output format (TSV, CSV, or JSON)
    :param include_annotations: include axiom annotations as additional columns when present
    :param predicates: list of predicates (ID or label) to include as columns in output -
                       if not specified, default_value_format is included as the first column to
                       identify the term
    :param sep: character to separate multiple cell values
    :param standalone: if True, include HTML root & headers
    :param statement: name of ontology statement table
    :param terms: list of terms (ID or label) to include as rows in output - if not specified,
                  all subjects from the statement table are included in output
    :param value_formats: optional dict of predicate ID to value format for output
                          (CURIE, IRI, or LABEL) - values for predicates not in this dict will be
                          rendered in the default_value_format
    :return: string output in given format
    """
    # We only need to get prefixes if we need to render any IRIs, or if fmt is HTML
    # (prefixes needed for RDFa)
    prefixes = {}
    if (
        fmt == "html"
        or default_value_format == "IRI"
        or (predicates and "IRI" in predicates)
        or (value_formats and "IRI" in value_formats.values())
    ):
        prefixes = get_prefixes(conn)

    # Use predicates to determine what values we need to export
    if predicates:
        # If the user provided a set of predicates, the output should include each of these
        # even if there are no values
        incl_predicates = True

        # Check for export-specific headers: IRI, CURIE, or LABEL
        export_predicates = [x for x in predicates if x in ["IRI", "CURIE", "LABEL"]]
        ontology_predicates = [x for x in predicates if x not in export_predicates]

        # Get the IDs of the remaining predicates (dict of input -> ID)
        # We use this to help us maintain the order that the predicates were passed in for output
        predicate_label_to_id = get_ids(
            conn,
            id_or_labels=ontology_predicates,
            id_type="predicate",
            return_dict=True,
            statement=statement,
        )
        predicate_ids = list(predicate_label_to_id.values())
        predicate_labels = {predicate_label_to_id.get(p, p): p for p in predicates}

        # Table headers are all predicate labels - included even if there are no values
        headers = list(predicate_labels.values())

        # Fix special LABEL case
        if "LABEL" in predicate_labels:
            del predicate_labels["LABEL"]
            predicate_label_to_id["LABEL"] = "rdfs:label"
            predicate_labels["rdfs:label"] = "LABEL"
            predicate_ids.append("rdfs:label")
    else:
        # If no predicates were provided, we only include predicates with values in the output
        incl_predicates = False

        # Get the IDs of all predicates
        predicate_ids = get_ids(conn, id_type="predicate", statement=statement)

        # Predicates should be rendered in default value format
        if default_value_format == "LABEL":
            predicate_labels = get_labels(conn, predicate_ids, statement=statement)
            predicate_labels["rdfs:label"] = "LABEL"
        elif default_value_format == "IRI":
            predicate_labels = {p: get_iri(prefixes, p) for p in predicate_ids}
        else:
            predicate_labels = {p: p for p in predicate_ids}

        # Add the default value format to labels
        if default_value_format == "LABEL":
            predicate_labels["rdfs:label"] = "LABEL"
        else:
            predicate_labels[default_value_format] = default_value_format

        # Reverse the dict for getting ID from label
        predicate_label_to_id = {v: k for k, v in predicate_labels.items()}

        # Table headers are not set until after getting the data
        # ... so we know to exclude predicates that do not have any values
        headers = None

    # Get the set of terms to export
    term_ids = None
    if terms:
        # Current terms are IDs or labels - make sure we get all the IDs
        term_ids = get_ids(conn, id_or_labels=terms, statement=statement)
        if not term_ids:
            raise ValueError(f"No terms provided exist in table '{statement}'!")
    data = get_objects(
        conn,
        predicate_ids,
        exclude_json=True,
        include_all_predicates=incl_predicates,
        statement=statement,
        term_ids=term_ids,
    )

    # Render the data as list of dicts
    rendered = terms2dicts(
        conn,
        data,
        default_value_format=default_value_format,
        include_annotations=include_annotations,
        include_id=True,
        prefixes=prefixes,
        rdfa=fmt == "html",
        sep=sep,
        statement=statement,
        value_formats=value_formats,
    )
    if not headers:
        # Only include headers for predicates with values, sorted alphabetically
        headers = sorted(
            [predicate_labels.get(x, x) for x in set().union(*(d.keys() for d in rendered))]
        )
        if "ID" in headers:
            headers.remove("ID")
        # Then move the default value format to the first position
        headers.insert(0, default_value_format)

    # Find annotation predicates, as we need to label these and insert them into the headers
    if include_annotations:
        # Map each predicate used to a list of annotation predicates
        annotation_predicates = set()
        predicate_to_annotation = defaultdict(set)
        for r in rendered:
            for predicate_id, value in r.items():
                if isinstance(value, str):
                    continue
                if predicate_id not in predicate_to_annotation:
                    predicate_to_annotation[predicate_id] = set()
                if value.get("annotation"):
                    predicate_to_annotation[predicate_id].update(value["annotation"].keys())
                    annotation_predicates.update(value["annotation"].keys())

        # Get the labels for these annotation predicates
        if default_value_format == "LABEL":
            annotation_predicate_labels = get_labels(
                conn, list(annotation_predicates), statement=statement
            )
        elif default_value_format == "IRI":
            annotation_predicate_labels = {x: get_iri(prefixes, x) for x in annotation_predicates}
        else:
            annotation_predicate_labels = {x: clean_object(x) for x in annotation_predicates}

        # Then insert them into the headers following the correct predicate
        headers_update = []
        for h in headers:
            headers_update.append(h)
            annotation_predicates = predicate_to_annotation.get(predicate_label_to_id.get(h, h))
            if annotation_predicates:
                headers_update.extend(
                    ["> " + annotation_predicate_labels[x] for x in annotation_predicates]
                )
        headers = headers_update
        all_predicate_labels = annotation_predicate_labels.copy()
        all_predicate_labels.update(predicate_labels)
    else:
        all_predicate_labels = predicate_labels

    # Then replace the predicate IDs (keys) with the correct display header
    # - this may be label, CURIE, or IRI depending on default value format OR what was input
    rendered = replace_predicate_ids(
        rendered, headers, prefixes, all_predicate_labels, rdfa=fmt == "html"
    )

    # Render the output as string, either table or JSON
    if fmt == "tsv" or fmt == "csv":
        delimiter = "\t"
        if fmt == "csv":
            delimiter = ","
        return dicts2tsv(rendered, headers, delimiter=delimiter)
    elif fmt == "html":
        return dicts2rdfa(rendered, headers, prefixes, standalone=standalone)
    return json.dumps(rendered, indent=2)


def get_iri(prefixes: dict, curie: str):
    """Get the full IRI for a CURIE.

    :param prefixes: dict of prefix -> base
    :param curie: CURIE to expand
    :return: CURIE expanded as IRI, or CURIE if prefix cannot be found
    """
    if curie.startswith("<"):
        return curie[1:-1]
    prefix = curie.split(":")[0]
    base = prefixes.get(prefix)
    if not base:
        logging.error("No matching base for prefix in term " + curie)
        return curie
    return curie.replace(prefix + ":", base)


def replace_predicate_ids(
    data: list, headers: list, prefixes: dict, predicate_labels: dict, rdfa: bool = False
):
    """Replace the predicate IDs with the provided headers for the export output. These may be what
    was originally passed in, or the predicates rendered in the value format (IRI, CURIE, or LABEL).

    :param data: export data
    :param headers: list of headers in order
    :param prefixes: dict of prefix -> base
    :param predicate_labels: dict of predicate ID -> label
    :param rdfa: set to True when the values of the predicate_objects dicts are hiccup lists for RDFa
    :return: export data with output predicate labels
    """
    # Reverse predicate_label dict to get ID from label
    predicate_label_to_id = {v: k for k, v in predicate_labels.items()}
    data_fixed = []
    for itm in data:
        itm_fixed = {}
        term_id = itm["ID"]
        for predicate_label in headers:
            predicate_id = predicate_label_to_id.get(predicate_label, predicate_label)
            if predicate_id == "IRI":
                if rdfa:
                    itm_fixed["IRI"] = ["p", get_iri(prefixes, term_id)]
                else:
                    itm_fixed["IRI"] = get_iri(prefixes, term_id)
            elif predicate_id in ["CURIE", "ID"]:
                if rdfa:
                    itm_fixed[predicate_id] = ["p", term_id]
                else:
                    itm_fixed[predicate_id] = term_id
            else:
                value = itm.get(predicate_id)
                if rdfa:
                    # item is a hiccup list, we don't need to do anything
                    itm_fixed[predicate_label] = value
                    continue
                if value:
                    itm_fixed[predicate_label] = value["value"]
                    annotation = value.get("annotation", {})
                    for ann_predicate, ann_value in annotation.items():
                        ann_predicate_label = "> " + predicate_labels[ann_predicate]
                        itm_fixed[ann_predicate_label] = ann_value
        data_fixed.append(itm_fixed)
    return data_fixed


def terms2dicts(
    conn: Connection,
    data: dict,
    default_value_format: str = "LABEL",
    include_annotations: bool = False,
    include_id: bool = True,
    prefixes: dict = None,
    rdfa: bool = False,
    sep: str = "|",
    single_item_list: bool = False,
    statement: str = "statement",
    value_formats: dict = None,
):
    """Convert each term detail object to a dict of predicate -> rendered object
    (either dict with value & annotation or hiccup list).

    :param conn: database connection
    :param data: term detail data
    :param default_value_format: default format to render terms (LABEL, CURIE, IRI)
    :param include_annotations: if True, include the annotations on predicate-object pairs
    :param include_id: if True, include an "ID" field in each term dict
    :param prefixes: dict of prefix->base
    :param rdfa: if True, render values as hiccup-style RDFa lists
    :param sep: character to separate multiple values for a single predicate
    :param single_item_list: if True, render objects as hiccup lists even if there is only one
                             element. Otherwise, we will only render a list for two or more objects.
    :param statement: name of ontology statement table
    :param value_formats: dict of predicate ID to the format to use for values (LABEL, CURIE, IRI)
    :return: list of term dicts with rendered objects
    """
    if not value_formats:
        value_formats = {}

    # First pass to render as OFN list and get all the needed term IDs for labeling
    pre_render, object_ids = pre_render_objects(data)

    # Get labels and entity types for Manchester rendering
    object_ids = list(object_ids)
    labels = get_labels(conn, object_ids, statement=statement)
    # Only create CURIE and IRI dicts if we'll need them as value formats
    curies = None
    if default_value_format == "CURIE" or "CURIE" in value_formats.values():
        curies = {x: clean_object(x) for x in object_ids}
    iris = None
    if default_value_format == "IRI" or "IRI" in value_formats.values():
        iris = {x: get_iri(prefixes, x) for x in object_ids}
    entity_types = get_entity_types(conn, object_ids, statement=statement)

    rendered = []
    for term_id, predicate_objects in pre_render.items():
        rendered_term = {}
        if include_id:
            rendered_term["ID"] = term_id
        for predicate, objs in predicate_objects.items():
            # Determine which dict to use as "labels"
            value_format = value_formats.get(predicate, default_value_format)
            if value_format == "LABEL":
                use_labels = labels
            elif value_format == "CURIE":
                use_labels = curies
            else:
                use_labels = iris

            if rdfa:
                # Create a hiccup list, insert links, then render as string
                hiccup_lst = render_hiccup(
                    predicate,
                    objs,
                    use_labels,
                    entity_types,
                    include_annotations=include_annotations,
                    single_item_list=single_item_list,
                )
                rendered_term[predicate] = hiccup_lst
                continue

            # Otherwise just render as a string
            strs = [object2str(o, use_labels, entity_types) for o in objs]
            value = sep.join(sorted(strs))

            # Maybe add the annotations to the dict, or just add the value
            annotations = [x["annotation"] for x in objs if x.get("annotation")]
            if include_annotations and annotations:
                predicate_annotations = {}
                for ann_objs in annotations:
                    for ann_predicate, annotation in ann_objs.items():
                        # Check if this predicate is in value formats
                        # - if not, use the annotated predicate's value format
                        ann_value_format = value_formats.get(ann_predicate, value_format)
                        if ann_value_format == "LABEL":
                            use_labels = labels
                        elif ann_value_format == "CURIE":
                            use_labels = curies
                        else:
                            use_labels = iris
                        strs = [object2str(o, use_labels, entity_types) for o in annotation]
                        predicate_annotations[ann_predicate] = sep.join(sorted(strs))
                rendered_term[predicate] = {"value": value, "annotation": predicate_annotations}
            else:
                rendered_term[predicate] = {"value": value}
        rendered.append(rendered_term)
    return rendered


if __name__ == "__main__":
    main()
