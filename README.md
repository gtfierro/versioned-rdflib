# Versioned RDF

- `graph_at` doesn't  currently work
- ~~add `undo/redo` functions that actually mutate the graph~~
- ~~`undo` a particular transation?~~
- integrate into Brickschema
- TODOs:
    - incorporate research on how to do versioning:
        - https://rdfostrich.github.io/article-versioned-reasoning/
        - https://github.com/rdfostrich

## Example

```python
import time
import datetime
import logging
logging.basicConfig(level=logging.DEBUG)
from vrdf import DB
import pyshacl
import rdflib

BRICK = rdflib.Namespace("https://brickschema.org/schema/Brick#")
A = rdflib.RDF.type
BLDG = rdflib.Namespace("urn:bldg#")

db = DB("test.db")

# load in Brick ontology
with db.new_changeset("brick") as cs:
    cs.load_file(
            "https://sparql.gtf.fyi/ttl/Brick1.3rc1.ttl"
    )
print(f"Have {len(db)} triples")

# can add precommit and postcommit hooks to implement desired functionality
# precommit hooks are run *before* the transaction is committed but *after* all of
# the changes have been made to the graph.
# postcommit hooks are run *after* the transaction is committed.
def validate(graph):
    print("Validating graph")
    valid, _, report = pyshacl.validate(graph, advanced=True, allow_warnings=True)
    assert valid, report
# uncommenting the below line will cause the precommit hook to be called
# after every transaction. This is currently too slow for our example so
# we are disabling it.
# db.add_postcommit_hook(validate)

with db.new_changeset("my-building") as cs:
    # 'cs' is a rdflib.Graph that supports queries -- updates on it
    # are buffered in the transaction and cannot be queried until
    # the transaction is committed (at the end of the context block)
    cs.add((BLDG.vav1, A, BRICK.VAV))
    cs.add((BLDG.vav1, BRICK.feeds, BLDG.zone1))
    cs.add((BLDG.zone1, A, BRICK.HVAC_Zone))
    cs.add((BLDG.zone1, BRICK.hasPart, BLDG.room1))
print(f"Have {len(db)} triples")

snapshot = db.latest_version

with db.new_changeset("my-building") as cs:
    cs.remove((BLDG.zone1, A, BRICK.HVAC_Zone))
    cs.add((BLDG.zone1, A, BRICK.Temperature_Sensor))
print(f"Have {len(db)} triples")

# query the graph 3 seconds ago (before the latest commit)
ts = (datetime.datetime.now() - datetime.timedelta(seconds=6)).strftime("%Y-%m-%dT%H:%M:%S")
print("Loop through versions")
for v in db.versions():
    print(f"{v.timestamp} {v.id} {v.graph}")
g = db.graph_at(timestamp=snapshot)
print(f"Have {len(g)} triples")
res = g.query("SELECT * WHERE { ?x a brick:Temperature_Sensor }")
assert len(list(res)) == 0 # should be 0 because sensor not added yet
```
