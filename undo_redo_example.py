import logging
logging.basicConfig(level=logging.DEBUG)
from vrdf import DB
import pyshacl
import rdflib

BRICK = rdflib.Namespace("https://brickschema.org/schema/Brick#")
A = rdflib.RDF.type
BLDG = rdflib.Namespace("urn:bldg#")

db = DB(":memory:")

# load in Brick ontology v1.3rc1
with db.new_changeset("brick") as cs:
    cs.load_file(
            "https://sparql.gtf.fyi/ttl/Brick1.3rc1.ttl"
    )
print(f"Have {len(db)} triples")

with db.new_changeset("bldg") as cs:
    cs.add((BLDG.Building, A, BRICK.Building))
    cs.add((BLDG.Building, rdflib.RDFS.label, rdflib.Literal("My Building")))

with db.new_changeset("bldg") as cs:
    for i in range(1,5):
        cs.add((BLDG.Building, BRICK.hasPart, BLDG[f"Floor{i}"]))
        cs.add((BLDG[f"Floor{i}"], A, BRICK.Floor))
floors = list(db.query("SELECT * WHERE { ?f a brick:Floor }"))
assert len(floors) == 4, len(floors)

# undo adding the floors
db.undo()
floors = list(db.query("SELECT * WHERE { ?f a brick:Floor }"))
assert len(floors) == 0, len(floors)

# actually.... add them back!
db.redo()
floors = list(db.query("SELECT * WHERE { ?f a brick:Floor }"))
assert len(floors) == 4, len(floors)
