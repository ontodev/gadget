import csv
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
    p.add_argument("-f", "--format", help="Output format (tsv, csv, html)", default="tsv")
    p.add_argument("-s", "--split", help="Character to split multiple values on", default="|")
    p.add_argument(
        "-V",
        "--values",
        help="Default value format for cell values (default: label)",
        default="LABEL",
    )
    p.add_argument(
        "-a",
        "--include-annotations",
        action="store_true",
        help="Include annotations as additional columns when present",
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
            statement=args.statement,
            terms=terms,
            value_formats=value_formats,
        )
    )


def dicts2tsv(rendered_data: list, headers: list, delimiter: str = "\t",) -> str:
    """Transform data from SQLite database to a list of dicts suitable for DictWriters.

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
    statement: str = "statement",
    terms: list = None,
    value_formats: dict = None,
) -> str:
    """

    :param conn:
    :param default_value_format:
    :param fmt:
    :param include_annotations:
    :param predicates:
    :param sep:
    :param statement:
    :param terms:
    :param value_formats:
    :return:
    """
    prefixes = {}
    if default_value_format == "IRI" or (predicates and "IRI" in predicates):
        prefixes = get_prefixes(conn)
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
    rendered = replace_predicate_ids(rendered, headers, prefixes, all_predicate_labels)

    # Render the output as string, either table or JSON
    if fmt == "tsv" or fmt == "csv":
        delimiter = "\t"
        if fmt == "csv":
            delimiter = ","
        return dicts2tsv(rendered, headers, delimiter=delimiter)
    return json.dumps(rendered, indent=2)


def get_iri(prefixes, curie):
    """

    :param prefixes:
    :param curie:
    :return:
    """
    if curie.startswith("<"):
        return curie[1:-1]
    prefix = curie.split(":")[0]
    base = prefixes.get(prefix)
    if not base:
        logging.error("No matching base for prefix in term " + curie)
        return curie
    return curie.replace(prefix + ":", base)


def replace_predicate_ids(data, headers, prefixes, predicate_labels):
    """

    :param data:
    :param headers:
    :param prefixes:
    :param predicate_labels:
    :return:
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
                itm_fixed["IRI"] = get_iri(prefixes, term_id)
            elif predicate_id == "CURIE":
                itm_fixed["CURIE"] = term_id
            else:
                value = itm.get(predicate_id)
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
    hiccup: bool = False,
    include_annotations: bool = False,
    include_id: bool = True,
    prefixes: dict = None,
    sep: str = "|",
    single_item_list: bool = False,
    statement: str = "statement",
    value_formats: dict = None,
):
    """

    :param conn:
    :param data:
    :param default_value_format:
    :param hiccup:
    :param include_annotations:
    :param include_id:
    :param prefixes:
    :param sep:
    :param single_item_list:
    :param statement:
    :param value_formats: dict of predicate ID to the format to use for values (label, CURIE, IRI)
    :return:
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

    # Second pass to render the OFN as Manchester with labels
    if hiccup:
        return render_hiccup(
            pre_render,
            labels,
            entity_types,
            include_annotations=include_annotations,
            single_item_list=single_item_list,
        )

    rendered = []
    for term_id, predicate_objects in pre_render.items():
        rendered_term = {}
        if include_id:
            rendered_term["ID"] = term_id
        for predicate, objs in predicate_objects.items():
            value_format = value_formats.get(predicate, default_value_format)
            if value_format == "LABEL":
                strs = [object2str(o, labels, entity_types) for o in objs]
            elif value_format == "CURIE":
                strs = [object2str(o, curies, entity_types) for o in objs]
            else:
                strs = [object2str(o, iris, entity_types) for o in objs]
            value = sep.join(sorted(strs))
            annotations = [x["annotation"] for x in objs if x.get("annotation")]
            if include_annotations and annotations:
                predicate_annotations = {}
                for ann_objs in annotations:
                    for ann_predicate, annotation in ann_objs.items():
                        if value_format == "LABEL":
                            strs = [object2str(o, labels, entity_types) for o in annotation]
                        elif value_format == "CURIE":
                            strs = [object2str(o, curies, entity_types) for o in annotation]
                        else:
                            strs = [object2str(o, iris, entity_types) for o in annotation]
                        predicate_annotations[ann_predicate] = sep.join(sorted(strs))
                rendered_term[predicate] = {"value": value, "annotation": predicate_annotations}
            else:
                rendered_term[predicate] = {"value": value}
        rendered.append(rendered_term)
    return rendered


if __name__ == "__main__":
    main()
