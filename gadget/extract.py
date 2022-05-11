import csv
import logging
import sys

from argparse import ArgumentParser, Namespace
from sqlalchemy.engine.base import Connection
from sqlalchemy.sql.expression import text as sql_text
from .cli_helpers import get_connection, get_terms
from .sql import (
    escape_qnames,
    get_ancestor_hierarchy,
    get_children,
    get_descendant_hierarchy,
    get_ids,
    get_parents,
)

"""
Usage: python3 extract.py -d <sqlite-database> -t <curie> > <ttl-file>

Creates a new extract table containing the term(s), predicates, and ancestors. By default,
this table is called 'extract', but you can override with `-e <table>`/`--extract-table <table>`.
You can also include more than one `-t <curie>`/`--term <curie>`.

You may also specify multiple CURIEs to extract with `-T <file>`/`--terms <file>`,
where the file contains a list of CURIEs to extract.

You may also specify which predicates you would like to include with
`-p <curie>`/`--predicate <curie>` or `-P <file>`/`--predicates <file>`
where the file contains a list of predicate CURIEs.

Finally, if you don't wish to include the ancestors of the term/terms,
include the `-n`/`--no-hierarchy` flag.

The sqlite-database must be created by LDTab (https://github.com/ontodev/rdftab.rs).
If you created the table with a name other than the default 'statement' table,
make sure to specify this with `-S <table>`/`--statement <table>`

The CURIEs must use a prefix from the 'prefixes' table.
"""


def main():
    p = ArgumentParser()
    p.add_argument(
        "-d", "--database", required=True, help="Database file (.db) or configuration (.ini)"
    )
    p.add_argument(
        "-e", "--extract-table", help="Name for extract table (default: extract)", default="extract"
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
    p.add_argument(
        "-C",
        "--copy",
        action="append",
        help="Copy the values of a predicate to another predicate (--C <from> <to>)",
        nargs=2,
    )
    p.add_argument("-i", "--imports", help="TSV or CSV file containing import module details")
    p.add_argument("-c", "--config", help="Source configuration for imports")
    p.add_argument(
        "-I",
        "--intermediates",
        help="Included ancestor/descendant intermediates (default: all)",
        default="all",
    )
    p.add_argument(
        "-m", "--imported-from", help="IRI of source import ontology to annotate terms with"
    )
    p.add_argument(
        "-M",
        "--imported-from-property",
        help="ID of property to use for 'imported from' annotation (default: IAO:0000412)",
        default="IAO:0000412",
    )
    p.add_argument("-s", "--source", help="Ontology source to filter imports file")
    p.add_argument("-f", "--format", help="Output format (ttl or json)", default="ttl")
    p.add_argument(
        "-n",
        "--no-hierarchy",
        action="store_true",
        help="If provided, do not create any rdfs:subClassOf statements",
    )
    args = p.parse_args()
    run_extract(args)


def run_extract(args: Namespace):
    """Wrapper for CLI arg parsing to run extract.

    :param args: CLI args"""
    # Get required terms
    terms_list = get_terms(args.term, args.terms)
    terms = {}
    source = args.source
    if args.imports:
        terms = get_import_terms(args.imports, source=source)

    if not terms_list and not terms:
        logging.critical("One or more term(s) must be specified with --term, --terms, or --imports")
        sys.exit(1)

    for t in terms_list:
        if not args.no_hierarchy:
            terms[t] = {"Related": "ancestors"}
        else:
            terms[t] = {}

    predicates = get_terms(args.predicate, args.predicates)
    intermediates = args.intermediates
    imported_from = args.imported_from

    if args.config:
        # Get options from the config file based on the source
        if not source:
            logging.critical("A --source is required when using the --config option")
            sys.exit(1)
        config_path = args.config
        sep = "\t"
        if config_path.endswith(".csv"):
            sep = ","
        # Search for the source in the file and read in option
        found_source = False
        with open(config_path, "r") as f:
            reader = csv.DictReader(f, delimiter=sep)
            for row in reader:
                if row["Source"] == source:
                    found_source = True
                    intermediates = row.get("Intermediates", "all")
                    predicates_str = row.get("Predicates")
                    imported_from = row.get("IRI")
                    if predicates_str:
                        # Extend any existing command-line predicates
                        predicates.extend(predicates_str.split(" "))
                    break
        if not found_source:
            # No source with provided name found
            logging.critical(f"Source '{source}' does not exist in config file: " + config_path)
            sys.exit(1)

    # Get the database connection & extract terms to new table
    conn = get_connection(args.database)
    extract(
        conn,
        args.extract_table,
        copy_predicates=args.copy,
        imported_from=imported_from,
        imported_from_property=args.imported_from_property,
        intermediates=intermediates,
        no_hierarchy=args.no_hierarchy,
        predicates=predicates,
        statement=args.statement,
        terms=terms,
    )


def clean(conn: Connection, extract_table: str = None):
    """Remove temporary tables and, if included, the extract table.

    :param conn: database connection
    :param extract_table: name of extract table - if included, this table will be dropped"""
    conn.execute("DROP TABLE IF EXISTS tmp_terms")
    conn.execute("DROP TABLE IF EXISTS tmp_predicates")
    if extract_table:
        # TODO: warn or fail if table alraedy exists?
        conn.execute(f'DROP TABLE IF EXISTS "{extract_table}"')


def create_tables(
    conn: Connection,
    extract_table: str,
    terms: dict,
    predicate_ids: list,
    copy_predicates: list = None,
    imported_from: str = None,
    imported_from_property: str = "IAO:0000412",
    no_hierarchy: bool = False,
    statement: str = "statement",
):
    """Create a series of tables for the extract. First, we create two temp tables:
    - tmp_terms
    - tmp_predicates
    Then, we use these tables to create the extract_table.

    :param conn: database connection
    :param extract_table: name of table to insert extract module into
    :param terms: terms to extract from statement table
    :param predicate_ids: list of predicates to include in module
    :param copy_predicates: list of tuples (from, to) to copy values to a different predicate
    :param imported_from: IRI of ontology that these terms are imported from
    :param imported_from_property: predicate to use with imported_from (IAO:0000412)
    :param no_hierarchy: if True, do not assert parents unless an override parent is included
    :param statement: name of ontology statement table to extract from"""

    # Create the terms table containing parent -> child relationships
    conn.execute("CREATE TABLE tmp_terms(child TEXT, parent TEXT)")
    for term_id in terms.keys():
        query = sql_text("INSERT INTO tmp_terms VALUES (:term_id, NULL)")
        conn.execute(query, term_id=term_id)

    # Create tmp predicates table containing all predicates to include
    conn.execute("CREATE TABLE tmp_predicates(predicate TEXT PRIMARY KEY NOT NULL)")
    if predicate_ids:
        for predicate_id in predicate_ids:
            if str(conn.engine.url).startswith("sqlite"):
                query = sql_text("INSERT OR IGNORE INTO tmp_predicates VALUES (:predicate_id)")
                conn.execute(query, predicate_id=predicate_id)
            else:
                query = sql_text(
                    """INSERT INTO tmp_predicates VALUES (:predicate_id)
                    ON CONFLICT (predicate) DO NOTHING"""
                )
                conn.execute(query, predicate_id=predicate_id)
    else:
        # Insert all predicates
        if str(conn.engine.url).startswith("sqlite"):
            conn.execute(
                f"""INSERT OR IGNORE INTO tmp_predicates
                     SELECT DISTINCT predicate
                     FROM {statement} WHERE predicate NOT IN
                       ('rdfs:subClassOf', 'rdfs:subPropertyOf', 'rdf:type')"""
            )
        else:
            conn.execute(
                f"""INSERT INTO tmp_predicates
                     SELECT DISTINCT predicate
                     FROM {statement} WHERE predicate NOT IN
                       ('rdfs:subClassOf', 'rdfs:subPropertyOf', 'rdf:type')
                     ON CONFLICT (predicate) DO NOTHING"""
            )

    # First pass through terms to collect any that need parents
    assign_parents = []
    for term_id, details in terms.items():
        if no_hierarchy:
            continue
        override_parent = details.get("Parent ID")
        if override_parent:
            continue
        assign_parents.append(term_id)

    # We get the hierarchy for all these terms at the same time to save time
    hierarchy = {}
    if assign_parents:
        hierarchy = get_ancestor_hierarchy(conn, assign_parents, statement=statement)

    # Add subclass/subproperty/type relationships to terms table
    for term_id, details in terms.items():
        # Check for overrides, regardless of no-hierarchy
        override_parent = details.get("Parent ID")
        if override_parent:
            # Just assert this as parent and don't worry about existing parent(s)
            query = sql_text("INSERT INTO tmp_terms VALUES (:term_id, :override_parent)")
            conn.execute(query, term_id=term_id, override_parent=override_parent)
            continue
        if no_hierarchy:
            continue

        # Otherwise only add the parent if we want a hierarchy
        # Check for the first ancestor we can find with all terms considered "top level"
        # In many cases, this is just the direct parent
        parents = get_top_ancestors(hierarchy, term_id, top_terms=list(terms.keys()))
        parents = parents.intersection(set(terms.keys()))
        if parents:
            # Maintain these relationships in the import module
            for p in parents:
                if p == term_id:
                    continue
                query = sql_text("INSERT INTO tmp_terms VALUES (:term_id, :p)")
                conn.execute(query, term_id=term_id, p=p)

    # Create our extract table to hold the actual triples
    conn.execute(
        f"""CREATE TABLE "{extract_table}" (
             assertion INT NOT NULL,
             retraction INT NOT NULL DEFAULT 0,
             graph TEXT NOT NULL,
             subject TEXT NOT NULL,
             predicate TEXT NOT NULL,
             object TEXT NOT NULL,
             datatype TEXT NOT NULL,
             annotation TEXT
           )"""
    )

    # Insert rdf:type declarations - only for OWL entities
    conn.execute(
        f"""INSERT INTO "{extract_table}"
             SELECT * FROM "{statement}"
             WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
               AND predicate = 'rdf:type'
               AND object IN
               ('owl:Class',
                'owl:AnnotationProperty',
                'owl:DataProperty',
                'owl:ObjectProperty',
                'owl:NamedIndividual')"""
    )

    # Insert subproperty statements for any property types
    conn.execute(
        f"""INSERT INTO "{extract_table}" (assertion, graph, subject, predicate, object, datatype)
         SELECT DISTINCT 1, 'graph', child, 'rdfs:subPropertyOf', parent, '_IRI'
         FROM tmp_terms WHERE parent IS NOT NULL AND child IN
           (SELECT subject FROM "{statement}" WHERE predicate = 'rdf:type'
            AND object IN ('owl:AnnotationProperty', 'owl:DataProperty', 'owl:ObjectProperty'))"""
    )

    # Insert subclass statements for any class types
    conn.execute(
        f"""INSERT INTO "{extract_table}" (assertion, graph, subject, predicate, object, datatype)
         SELECT DISTINCT 1, 'graph', child, 'rdfs:subClassOf', parent, '_IRI'
         FROM tmp_terms WHERE parent IS NOT NULL AND child IN
           (SELECT subject FROM "{statement}"
            WHERE predicate = 'rdf:type' AND object = 'owl:Class')
        """
    )

    # Everything else is an instance
    # TODO: or datatype?
    conn.execute(
        f"""INSERT INTO "{extract_table}" (assertion, graph, subject, predicate, object, datatype)
        SELECT DISTINCT 1, 'graph', child, 'rdf:type', parent, '_IRI'
        FROM tmp_terms WHERE parent IS NOT NULL AND child NOT IN
          (SELECT subject from "{extract_table}"
           WHERE predicate IN ('rdfs:subClassOf', 'rdfs:subPropertyOf'))"""
    )

    # Insert literal annotations
    conn.execute(
        f"""INSERT INTO "{extract_table}"
        SELECT * FROM "{statement}"
        WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
          AND predicate IN (SELECT predicate FROM tmp_predicates)
          AND object IS NOT NULL
          AND datatype NOT IN ('_IRI', '_JSON')"""
    )

    # Insert logical relationships (object must be in set of input terms)
    conn.execute(
        f"""INSERT INTO "{extract_table}"
        SELECT * FROM "{statement}"
        WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
          AND predicate IN (SELECT predicate FROM tmp_predicates)
          AND object IN (SELECT DISTINCT child FROM tmp_terms)"""
    )

    # Insert IRI annotations (object does not have to be in input terms)
    conn.execute(
        f"""INSERT INTO "{extract_table}" (assertion, graph, subject, predicate, object, datatype)
        SELECT 1, 'graph', s1.subject, s1.predicate, s1.object, '_IRI' FROM "{statement}" s1
        JOIN "{statement}" s2 ON s1.predicate = s2.subject
        WHERE s1.subject IN (SELECT DISTINCT child FROM tmp_terms)
          AND s1.predicate IN (SELECT predicate FROM tmp_predicates)
          AND s2.object = 'owl:AnnotationProperty'
          AND s1.datatype = '_IRI'"""
    )

    # If imported_from IRI is included, add this to add terms
    if imported_from:
        query = sql_text(
            f"""INSERT INTO "{extract_table}"
                (assertion, graph, subject, predicate, object, datatype)
            SELECT DISTINCT 1, 'graph', child, :imported_from_property, :imported_from, '_IRI'
            FROM tmp_terms"""
        )
        conn.execute(
            query, imported_from_property=imported_from_property, imported_from=f"<{imported_from}>"
        )

    # Finally, copy predicates ("from" does not need to be in current extract)
    if copy_predicates:
        for from_predicate, to_predicate in copy_predicates:
            query = sql_text(
                f"""INSERT INTO "{extract_table}"
                    (assertion, graph, subject, predicate, object, datatype)
                SELECT assertion, graph, subject, '{to_predicate}', object, datatype
                FROM "{statement}"
                WHERE subject IN (SELECT DISTINCT child FROM tmp_terms)
                AND predicate = :from_predicate"""
            )
            conn.execute(query, from_predicate=from_predicate)


def extract(
    conn: Connection,
    extract_table: str,
    copy_predicates: list = None,
    imported_from: str = None,
    imported_from_property: str = "IAO:0000412",
    intermediates: str = "all",
    no_hierarchy: bool = False,
    predicates: list = None,
    statement: str = "statement",
    terms: dict = None,
):
    """Extract terms from the ontology database and return the module as Turtle or JSON-LD."""
    intermediates = intermediates.lower()
    if intermediates not in ["all", "none"]:
        raise Exception("Unknown 'intermediates' option: " + intermediates)

    # Maybe get the related entities from the import file
    # ... these are extra terms to add to the extracted module
    more_terms = get_related_entities(conn, terms, intermediates=intermediates, statement=statement)

    # Add those extra terms from related entities to our terms dict
    for mt in more_terms:
        if mt not in terms:
            # Don't worry about the parent ID because hierarchy will be maintained ...
            # ... based on the first ancestor in the full set of terms
            terms[mt] = {}

    predicate_ids = None
    if predicates:
        # Current predicates are IDs or labels - make sure we get all the IDs
        predicate_ids = get_ids(conn, predicates, statement=statement)

    # Pre-build clean up - this will remove any existing extract table
    clean(conn, extract_table=extract_table)
    try:
        # Build temp tables & extract table (not temp)
        create_tables(
            conn,
            extract_table,
            terms,
            predicate_ids,
            copy_predicates=copy_predicates,
            imported_from=imported_from,
            imported_from_property=imported_from_property,
            no_hierarchy=no_hierarchy,
            statement=statement,
        )
    finally:
        # Post clean up - only remove temp tables
        clean(conn)
    # Clean up QNames for exporting to TTL
    escape_qnames(conn, extract_table)


def get_all_descendants(hierarchy: dict, term_id: str, descendants: set = None):
    """Get a set of all descendants for a term.

    :param hierarchy: dict of parent -> list of children
    :param term_id: term to get the descedants of (recursive)
    :param descendants: set to add descendants to
    :return set of all descendants"""
    if not descendants:
        descendants = set()
    children = hierarchy.get(term_id, [])
    for c in children:
        descendants.add(c)
        descendants.update(get_all_descendants(hierarchy, c, descendants=descendants))
    return descendants


def get_bottom_descendants(hierarchy: dict, term_id: str, descendants: set = None) -> set:
    """Get all bottom-level descendants for a given term with no intermediates. The bottom-level
    terms are those that are not ever used as the object of an rdfs:subClassOf statement.

    :param hierarchy: dict containing child->parent relationships
    :param term_id: term ID to get the bottom descendants of (recursive)
    :param descendants: a set to add descendants to
    :return set of bottom-level descendants
    """
    if not descendants:
        descendants = set()
    children = hierarchy.get(term_id)
    if not children:
        descendants.add(term_id)
    else:
        for c in children:
            descendants.update(get_bottom_descendants(hierarchy, c, descendants=descendants))
    return descendants


def get_hierarchy_capped(
    full_hierarchy: dict,
    top_terms: set,
    term_id: str,
    capped_hierarchy: set = None,
) -> set:
    """Get a set of all ancestors, in order, up to the top terms. If a lineage does not contain a
    top term, it will go all the way to the top ancestor (no asserted parent).

    :param full_hierarchy: dict of child -> list of parents
    :param top_terms: set of terms to stop lineage at
    :param term_id: term to get ancestor hierarchy of (recursive)
    :param capped_hierarchy: set to add ancestors to
    :return set of ancestors up to top term (or top ancestor)"""
    if not capped_hierarchy:
        capped_hierarchy = set()
    parents = full_hierarchy.get(term_id, [])
    for p in parents:
        if p == "owl:Thing":
            continue
        elif top_terms and p in top_terms:
            capped_hierarchy.add(p)
            continue
        else:
            # parent is not a top_term, add it to our set and go to next level
            capped_hierarchy.add(p)
            capped_hierarchy.update(
                get_hierarchy_capped(
                    full_hierarchy, top_terms, p, capped_hierarchy=capped_hierarchy,
                )
            )
    return capped_hierarchy


def get_import_terms(import_file: str, source: str = None) -> dict:
    """Get the terms and their details from the input file.

    :param import_file: path to file containing import details
    :param source: source ontology ID for terms to include
                   (if None or if input has no 'source' column, all are included)
    :return dict of term ID -> details
    """
    terms = {}
    sep = "\t"
    if import_file.endswith(".csv"):
        sep = "\t"
    with open(import_file, "r") as f:
        reader = csv.DictReader(f, delimiter=sep)
        for row in reader:
            term_id = row.get("ID")
            if not term_id:
                continue
            if source and row.get("Source") != source:
                # If we have a source and this is not from that source, skip
                continue
            terms[term_id] = {"Parent ID": row.get("Parent ID"), "Related": row.get("Related")}
    return terms


def get_related_entities(conn: Connection, terms: dict, intermediates: str = "all", statement: str = "statement") -> set:
    """Get the related entities for the input terms that have 'Related' entries.

    :param conn: database connection
    :param terms: dict of term ID to details (including if they need related entities)
    :param intermediates: intermediate entities to include from ancestor/descendant relation
                          ('all' or 'none')
    :param statement: name of ontology statement table
    :return set of extra terms to add to extracted module"""
    # First pass to get collect terms that we need hierarchies for (ancestors/descendants)
    # so that we can get the full hierarchies all at once to save time
    ancestor_hierarchy_terms = []
    descendant_hierarchy_terms = []
    related_entities = {}
    for term_id, details in terms.items():
        related = details.get("Related")
        if not related:
            continue
        related = related.strip().lower().split(" ")
        related_entities[term_id] = related
        if "ancestors" in related:
            ancestor_hierarchy_terms.append(term_id)
        elif "descendants" in related:
            descendant_hierarchy_terms.append(term_id)
    ancestor_hierarchy = {}
    descendant_hierarchy = {}
    if ancestor_hierarchy_terms:
        ancestor_hierarchy = get_ancestor_hierarchy(
            conn, ancestor_hierarchy_terms, statement=statement
        )
    if descendant_hierarchy_terms:
        descendant_hierarchy = get_descendant_hierarchy(
            conn, descendant_hierarchy_terms, statement=statement
        )

    # Second pass on those with related entities
    # this time we actually add the related entities to a set using the hierarchies
    more_terms = set()
    for term_id, related in related_entities.items():
        for r in related:
            if r == "ancestors":
                # TODO: collect these first and get the hierarchy at the same time
                if intermediates == "none":
                    # Find first ancestor/s that is/are either:
                    # - in the set of input terms
                    # - a top level term (below owl:Thing)
                    ancestors = get_top_ancestors(ancestor_hierarchy, term_id,
                                                  top_terms=list(terms.keys()))
                else:
                    # Otherwise get a set of ancestors, stopping at terms that are either:
                    # - in the set of input terms
                    # - a top level term (below owl:Thing)
                    ancestors = get_hierarchy_capped(ancestor_hierarchy, set(terms.keys()), term_id)
                more_terms.update(ancestors)
            elif r == "children":
                # Just add the direct children
                more_terms.update(get_children(conn, term_id, statement=statement))
            elif r == "descendants":
                if intermediates == "none":
                    # Find all bottom-level descendants (do not have children)
                    descendants = get_bottom_descendants(descendant_hierarchy, term_id)
                    more_terms.update(descendants)
                else:
                    # Get a set of all descendants, including intermediates
                    more_terms.update(get_all_descendants(descendant_hierarchy, term_id))
            elif r == "parents":
                # Just add the direct parents
                more_terms.update(get_parents(conn, term_id, statement=statement))
            else:
                # TODO: should this just warn and continue?
                raise Exception(f"unknown 'Related' keyword for '{term_id}': " + r)
    return more_terms


def get_top_ancestors(
    hierarchy: dict,
    term_id: str,
    top_ancestors: set = None,
    top_terms: list = None,
) -> set:
    """Get a set of top-level ancestors (either in top terms or ancestors with no asserted parents).

    :param hierarchy: dict containing child -> list of parents
    :param term_id: term to retrieve top ancestors for
    :param top_ancestors: set to collect top ancestors in
    :param top_terms: set of terms to consider as top ancestors
    :return set of top ancestors"""
    if not top_ancestors:
        top_ancestors = set()

    parents = hierarchy.get(term_id)
    if not parents:
        top_ancestors.add(term_id)
    else:
        for p in parents:
            if p == "owl:Thing":
                top_ancestors.add(term_id)
            elif p in top_terms:
                top_ancestors.add(p)
            else:
                top_ancestors.update(
                    get_top_ancestors(hierarchy, p, top_ancestors=top_ancestors, top_terms=top_terms)
                )
    return top_ancestors


if __name__ == "__main__":
    main()
