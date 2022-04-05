# Gadget

Utilites for ontology development using linked data tables ([LDTab](https://github.com/ontodev/ldtab.clj))

## Setup

In addition to the requirements in `requirements.txt`, `gadget` also needs [`wiring.py`](https://github.com/ontodev/wiring.py). Follow the instructions for `wiring.py` setup, then install these requirements in the same virtual environment.

To install `wiring.py` outside of a virtual environment:
1. Install `maturin` (`pip install -U pip maturin`)
2. Navigate to `wiring.py/wiring.rs`
3. Build the wheel (`maturin build`)
4. Install the wheel (`pip install --force-reinstall target/wheels/wiring_rs-*.whl`)


## Usage

`gadget` operates on a linked data table ([LDTab](https://github.com/ontodev/ldtab.clj)). You must also define the following views before using any `gadget` utilities. `STATEMENT` in all queries below should be replaced with the name of your statement table.

### Search View

This view creates a table containing ID -> search name. This view is only required for `gadget.search`. The search name is formatted as "LABEL - SYNONYM [TERM ID]". Be sure to replace `PREDICATE_ID` with the predicate used for synonyms, e.g. `IAO:0000118` (alternative term). You can add additional synonyms to the `synonyms` CTE with one or more `UNION` statements: `UNION SELECT DISTINCT subject, object FROM STATEMENT WHERE predicate = 'PREDICATE_ID'`

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
