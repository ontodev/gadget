import csv

from io import StringIO
from sqlalchemy.engine.base import Connection
from .render import object2str, pre_render_objects, render_hiccup
from .sql import get_entity_types, get_labels


def terms2dict(
    conn: Connection,
    data: dict,
    hiccup: bool = False,
    include_annotations: bool = False,
    sep: str = "|",
    single_item_list: bool = False,
    statement: str = "statement",
):
    # First pass to render as OFN list and get all the needed term IDs for labeling
    pre_render, object_ids = pre_render_objects(data)

    # Get labels and entity types for Manchester rendering
    object_ids = list(object_ids)
    labels = get_labels(conn, object_ids, statement=statement)
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
        rendered_term = {"ID": term_id}
        for predicate, objs in predicate_objects.items():
            pred_label = labels.get(predicate, predicate)
            strs = [object2str(o, labels, entity_types) for o in objs]
            rendered_term[pred_label] = sep.join(strs)
        rendered.append(rendered_term)
    return rendered


def terms2tsv(
    conn: Connection,
    data: dict,
    delimiter: str = "\t",
    sep: str = "|",
    statement: str = "statement",
) -> str:
    """Transform data from SQLite database to a list of dicts suitable for DictWriters.

    :param conn: database connection to query for labels and entity types
    :param data: data from query
    :param delimiter: character to separate cells (default '\t' for TSV)
    :param sep: separator for multiple values in a single cell
    :param statement: name of ontology statement table
    :return: list of dicts to write to table using csv.DictWriter
    """
    rendered = terms2dict(conn, data, sep=sep, statement=statement)
    headers = rendered[0].keys()
    output = StringIO()
    writer = csv.DictWriter(output, delimiter=delimiter, fieldnames=headers, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rendered)
    return output.getvalue()
