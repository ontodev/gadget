# Gadget

Utilites for ontology development using linked data tables ([LDTab](https://github.com/ontodev/ldtab.clj))

## Setup

In addition to the requirements in `requirements.txt`, `gadget` also needs [`wiring.py`](https://github.com/ontodev/wiring.py) for `export` and `tree`. Follow the instructions for `wiring.py` setup, then install these requirements in the same virtual environment.

To install `wiring.py` outside of a virtual environment:
1. Install `maturin` (`pip install -U pip maturin`)
2. Navigate to `wiring.py/wiring.rs`
3. Build the wheel (`maturin build`)
4. Install the wheel (`pip install --force-reinstall target/wheels/wiring_rs-*.whl`)

## Usage

`gadget` operates on a linked data table ([LDTab](https://github.com/ontodev/ldtab.clj)).

### Databases

Each `gadget` module uses a SQL database version of an RDF or OWL ontology to create outputs. SQL database inputs should be created from OWL using [LDTab](https://github.com/ontodev/ldtab.clj) to ensure they are in the right format. LDTab creates SQLite databases, but we also support PostgreSQL. The database is specified by `-d`/`--database`. 

When loading from a SQLite database, use the path to the database (`foo.db`). When loading a PostgreSQL database, use a path to a configuration file ending in `.ini` (e.g., `conf.ini`) with a `[postgresql]` section. We recommend that the configuration file contain at least `database`, `user`, and `password` fields. An example configuration file with all optional fields looks like:
```
[postgresql]
host = 127.0.0.1
database = my_ontology
user = postgres
password = secret_password
port = 5432
```

The following prefixes are required to be defined in the `prefix` table for the `gadget` commands to work:
* `owl`: `http://www.w3.org/2002/07/owl#`
* `rdf`: `http://www.w3.org/1999/02/22-rdf-syntax-ns#`
* `rdfs`: `http://www.w3.org/2000/01/rdf-schema#`

When using `gadget` as a Python module, all operations accept a `sqlalchemy` Connection object. For details on the Connection, see [Working with Engines and Connections](https://docs.sqlalchemy.org/en/14/core/connections.html). We currently support SQLite and PostgreSQL. Using other connections may result in unanticipated errors due to slight variations in syntax. Note that if you use a PostgreSQL database, you must install or include `psycopg2` in your requirements.

### Search View

This view creates a table containing ID -> search name. This view is only required for `gadget.search`. The search name is formatted as "LABEL - SYNONYM [TERM ID]". Be sure to replace `PREDICATE_ID` with the predicate used for synonyms, e.g. `IAO:0000118` (alternative term), and `STATEMENT` should be replaced with the name of your statement table. You can add additional synonyms to the `synonyms` CTE with one or more `UNION` statements: `UNION SELECT DISTINCT subject, object FROM STATEMENT WHERE predicate = 'PREDICATE_ID'`

```sql
CREATE VIEW STATEMENT_search_view AS
WITH term_ids AS (
    SELECT * FROM (
        SELECT DISTINCT subject AS subject FROM STATEMENT
        UNION
        SELECT DISTINCT predicate FROM STATEMENT
    )
),
labels AS (
    SELECT DISTINCT subject, object
    FROM [STATEMENT] WHERE predicate = 'rdfs:label'
),
synonyms AS (
    SELECT * FROM (
        SELECT DISTINCT subject, object FROM STATEMENT
        WHERE predicate = 'PREDICATE_ID'
    )
)
SELECT
    t.subject AS subject,
    COALESCE(l.object, "") || COALESCE(" - " || s.object, "") || " [" || t.subject || "]" AS label
FROM term_ids t
LEFT JOIN labels l ON t.subject = l.subject
LEFT JOIN synonyms s ON t.subject = s.subject;
```

## CLI Usage

The following modules are also supported as CLI commands:
- [Export](#export)
- [Extract](#extract)

### Global Options

Each command expects a `-d <database>`/`--database <database>` option that provides the path to the database to operate on. You can also provide `-S <statement>`/`--statement <statement>` to specify the name of your ontology statement table, if it is not the default: `statement`.

#### Terms

The term or terms as CURIEs or labels are specified with `-t <term>`/`--term <term>`. You may also specify multiple terms with `-T <file>`/`--terms <file>`, where the file contains a list of CURIEs.

#### Predicates

You may also specify which predicates you would like to include with `-p <term>`/`--predicate <term>` or `-P <file>`/`--predicates <file>`, where the file contains a list of predicate CURIEs or labels. Otherwise, the output includes all predicates.

### Export

The `export` module creates a table (default TSV) output containing the terms and their predicates written to stdout.
```
python3 -m gadget.export -d [path-to-database] -S [statement-table] -t [term] > [output-tsv]
```

Export options:
* `-f <fmt>`/`--format <fmt>`: specify the format of the export (TSV, CSV, or JSON)
* `-s <char>`/`--split <char>`: specify the character to split multiple cell values on (`|`)
* `-V <values>`/`--values <values>`: specify the default value format, i.e., how cell values are rendered (CURIE, IRI, or LABEL)
    * Note: if you do not specify any predicates to include, the default value format option for each term will be included as the first column
* `-a`/`--include-annotation`: include annotations as additional columns followiing the predicate when present

### Extract

The `extract` module creates a new table called `extract` containing the term(s) and term ancestors up to `owl:Thing` as a new extracted module.
```
python3 -m gadget.extract -d [path-to-database] -S [statement-table] -t [term]
```

This table can then be exported to TTL using [LDTab](https://github.com/ontodev/ldtab.clj):
```
ldtab export -t extract [path-to-database] [output-ttl]
```

Extract options:
* `-e <table>`/`--extract-table <table>`: name of extract table to create
* `-n`/`--no-hierarchy`: do not include ancestors of the input term/s
* `-I <intermediates>`/`--intermediates <intermedaites>`: specify intermediate nodes to include (all or none)
* `-m <ontology-IRI>`/`--imported-from <ontology-IRI>`: annotate extracted terms with a source ontology IRI
* `-M <predicate>`/`--imported-from-property <predicate>`: property to use for `imported-from` annotation (['imported from'](http://purl.obolibrary.org/obo/IAO:0000412))
* `-i <file>`/`--imports <file>`: see below, [Creating Import Modules](#creating-import-modules)
* `-s <source>`/`--source <source>`: see below, [Creating Import Modules](#creating-import-modules)

#### Creating Import Modules

`gadget.extract` can also be used with import configuration files (`-i <file>`/`--imports <file>`):

```
python3 -m gadget.extract -d [path-to-database] -i [path-to-imports]
```

These files contain the terms you wish to include in your import module, along with some other details. The required headers are:
* **ID**: The term ID to include
* **Label**: optional; the term's label for documentation
* **Parent ID**: optional; a term ID of a parent to assert
* **Parent Label**: optional; the parent's label for documentation
* **Related**: optional; related entities to include
* **Source**: optional; a short ontology ID that specifies the source

Including the source can be useful when you have one file that you are using to create more than one import module. When you specify the **Source** column, you can use the `-s <source>`/`--source <source>` option in the command line. For example, if one of the sources in your import config is `obi`:

```
python3 -m gadget.extract -d obi.db -i imports.tsv -s obi
```

The **Related** column should be a space-separated list that can use zero or more of the following. When included, all terms that match the relationship from the input database will be included in the output:
* `ancestors`: Get all intermediate terms between the given term and it's first ancestor that is included in the input terms. If `--intermediates none`, get the first included ancestor OR the top-level ancestor if an ancestor is not included in the input terms.
* `children`: Include the direct children of the term, regardless of if they are included in the input terms or not.
* `descendants`: Get all intermediate terms between the given term and the lowest-level term (terms that do not have children). If `--intermediates none`, get all bottom-level terms only.
* `parents`: Include the direct parents of the term, regardless of if they are included in the input terms or not.

You can also pass a source configuration file that contains the options for each source ontology used in your imports file using `-c <file>`/`--config <file>`. Note that with this option, a `--source` is always required:

```
python3 -m gadget.extract -d obi.db -i imports.tsv -c config.tsv -s obi
```

This is a TSV or CSV with the following headers:
* **Source**: a short ontology ID that specifies the source; matches a source in the imports file
* **IRI**: optional; the IRI of the ontology to be added to each term as the value of an ['imported from'](http://purl.obolibrary.org/obo/IAO:0000412) statement
* **Intermediates**: optional; an `--intermediates` option: `all` or `none`
* **Predicates**: optional; a space-separated list of predicate IDs to include from the import

The config file can be useful for handling multiple imports with different options in a `Makefile`. If your imports all use the same `--intermediates` option and the same predicates, there is no need to specify a config file.
