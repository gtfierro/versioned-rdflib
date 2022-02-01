# Versioned RDF


```python
from vrdf import DB, BRICK, A
from rdflib import Namespace

BLDG = Namespace("urn:bldg#")
db = DB("test.db")

# using logical timestamps here (0, 1, 2, 3, ...). If these are
# ommitted it defaults to the current system time.
with db.new_changeset("my-building", 1) as cs:
    cs.add((BLDG.vav1, A, BRICK.VAV))
    cs.add((BLDG.vav1, BRICK.feeds, BLDG.zone1))
    cs.add((BLDG.zone1, A, BRICK.HVAC_Zone))
    cs.add((BLDG.zone1, BRICK.hasPart, BLDG.room1))

# WARNING: this is slow for now
with db.new_changeset("brick", 1) as cs:
    # 'cs' is a rdflib.Graph that supports queries -- updates on it
    # are buffered in the transaction and cannot be queried until
    # the transaction is committed (at the end of the context block)
    cs.load_file(
        "https://github.com/BrickSchema/Brick/releases/download/nightly/Brick.ttl"
    )

with db.new_changeset("my-building", 2) as cs:
    cs.add((BLDG.vav2, A, BRICK.VAV))
    cs.add((BLDG.vav2, BRICK.feeds, BLDG.zone1))
    cs.add((BLDG.zone2, A, BRICK.HVAC_Zone))
    cs.remove((BLDG.zone1, BRICK.hasPart, BLDG.room1))

with db.new_changeset("my-building", 3) as cs:
    cs.add((BLDG.vav2, A, BRICK.VAV))
    cs.add((BLDG.vav2, BRICK.feeds, BLDG.zone1))
    cs.add((BLDG.zone2, A, BRICK.HVAC_Zone))
    cs.remove((BLDG.zone1, BRICK.hasPart, BLDG.room1))

with db.new_changeset("my-building", 4) as cs:
    cs.remove((BLDG.vav2, BRICK.feeds, BLDG.zone1))
    cs.add((BLDG.vav2, BRICK.feeds, BLDG.zone2))

print("LATEST!")
for t in db.latest("my-building"):
    print(t)

for logical_ts in range(1, 5):
    print("LOGICAL TS:", logical_ts)
    for t in db.graph_at("my-building", logical_ts):
        print(t)

db.close()
```
