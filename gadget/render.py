import json
import wiring_rs

from collections import defaultdict
from html import escape as html_escape
from typing import Tuple


def clean_object(x: str):
    """Clean the named object of a statement - this simply involves removing angle brackets on IRIs.

    :param x: object to clean
    :return: object with brackets stripped, if they exist
    """
    if x.startswith("<") and x.endswith(">"):
        return x[1:-1]
    return x


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


def object2hiccup(
    predicate: str,
    obj: dict,
    labels: dict,
    entity_types: dict,
    as_list: bool = False,
    include_annotations: bool = False,
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
        typed = wiring_rs.ofn_typing(json.dumps(obj["object"]), entity_types)
        ele.append(json.loads(wiring_rs.object_2_rdfa(typed, labels)))
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
    if obj.get("annotation") and include_annotations:
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


def object2str(obj: dict, labels: dict, entity_types: dict) -> str:
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
        return labels.get(obj["object"], clean_object(obj["object"]))
    else:
        # TODO: datatypes?
        return obj["object"]


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
                annotation = obj.get("annotation")
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


def render_hiccup(
    predicate, objects, labels, entity_types, include_annotations=False, single_item_list=False
) -> list:
    """Render the list of objects as a hiccup list for HTML/RDFa output.

    :param predicate: predicate for these objects
    :param objects: list of objects to render as hiccup list
    :param labels: dict of ID -> term labels
    :param entity_types: dict of ID -> term types
    :param include_annotations: if True, include the annotations in the hiccup list
    :param single_item_list: if True, render all objects as HTML ul elements even if there is only
                             one object - otherwise, ul elements are only created for two+ objects
    :return: hiccup list for given objects
    """
    if len(objects) > 1 or single_item_list:
        lst = ["ul"]
        lst.extend(
            [
                object2hiccup(
                    predicate,
                    o,
                    labels,
                    entity_types,
                    as_list=True,
                    include_annotations=include_annotations,
                )
                for o in objects
            ]
        )
        return lst
    elif len(objects) == 1:
        return object2hiccup(
            predicate, objects[0], labels, entity_types, include_annotations=include_annotations,
        )
    return []
