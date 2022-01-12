from typing import Union
from datetime import datetime
import time
import uuid
import sqlite3
from contextlib import contextmanager
from rdflib import Namespace
from brickschema.namespaces import BRICK, A
from rdflib import Graph

BLDG = Namespace("urn:bldg#")

changeset_table_defn = """CREATE TABLE IF NOT EXISTS changesets (
    id TEXT,
    timestamp TIMESTAMP NOT NULL,
    graph TEXT NOT NULL,
    is_insertion BOOLEAN NOT NULL,
    subject TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object TEXT NOT NULL
);"""

triple_table_defn = """CREATE TABLE IF NOT EXISTS triples (
    id INTEGER PRIMARY KEY,
    graph TEXT NOT NULL,
    subject TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object TEXT NOT NULL
);"""
triple_unique_idx = """CREATE UNIQUE INDEX IF NOT EXISTS triple_unique_idx
    ON triples (graph, subject, predicate, object);"""

class Changeset():
    def __init__(self, graph_name):
        self.name = graph_name
        self.uid = uuid.uuid4()
        self.additions = []
        self.deletions = []

    def add(self, triple):
        self.additions.append(triple)

    def load_file(self, filename):
        g = Graph()
        g.parse(filename, format="turtle")
        for s, p, o in g.triples((None, None, None)):
            self.add((s, p, o))

    def remove(self, triple):
        self.deletions.append(triple)

    def __repr__(self):
        return f"Changeset {self.name} {self.uid}\n|- {len(self.additions)} additions\n|- {len(self.deletions)} deletions"

# TODO: make this a subclass of rdflib.Dataset or ConjunctiveGraph?
# maybe also support a "union" over multiple graphs?
# We can define several "virtual unions" and include them in the graph.
# Maybe a virtual union can be a union over graphs and other virtual unions?
class DB:
    def __init__(self, file_name):
        self.file_name = file_name
        self.conn = sqlite3.connect(self.file_name)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.conn.execute(changeset_table_defn)
        self.conn.execute(triple_table_defn)
        self.conn.execute(triple_unique_idx)

        self.latest_changed = True
        self._cached_graph = Graph()

    @contextmanager
    def new_changeset(self, graph: str, ts: Union[str,int]=None):
        with self.conn:
            cs = Changeset(graph)
            yield cs
            if ts is None:
                ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")
            # delta by the user. We need to invert the changes so that they are expressed as a "backward"
            # delta. This means that we save the deletions in the changeset as "inserts", and the additions
            # as "deletions".
            for triple in cs.deletions:
                self.conn.execute("INSERT INTO changesets VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (str(cs.uid), ts, graph, True, triple[0].n3(), triple[1].n3(), triple[2].n3()))
                self.conn.execute("DELETE FROM triples WHERE graph = ? AND subject = ? AND predicate = ? AND object = ?",
                    (graph, triple[0].n3(), triple[1].n3(), triple[2].n3()))
            for triple in cs.additions:
                self.conn.execute("INSERT INTO changesets VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (str(cs.uid), ts, graph, False, triple[0].n3(), triple[1].n3(), triple[2].n3()))
                self.conn.execute("INSERT OR IGNORE INTO triples VALUES (NULL, ?, ?, ?, ?)",
                    (graph, triple[0].n3(), triple[1].n3(), triple[2].n3()))
        print(f"Committed changeset: {cs}")
        self.latest_changed = True

    def triples(self, graph: str):
        for row in self.conn.execute("SELECT * FROM triples WHERE graph = ?", (graph,)):
            yield row

    def latest(self, graph: str) -> Graph:
        if not self.latest_changed:
            return self._cached_graph
        g = Graph()
        f = ""
        for row in self.conn.execute("SELECT * FROM triples WHERE graph = ?", (graph,)):
            f += f"{row['subject']} {row['predicate']} {row['object']} .\n"
        g.parse(data=f, format="turtle")
        self.latest_changed = False
        self._cached_graph = g
        return g

    def graph_at(self, graph: str, timestamp=None) -> Graph:
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")
        g = self.latest(graph)

        # producing the graph is a little wonky...we store the triples in separate fields,
        # but in order to load them into the graph, we create an in-memory n-triples file
        # and hand it to rdflib to parse. At this point, GitHub copilot suggested to add
        # the phrase "this is a hack but it works" to the comment.
        additions = ""
        deletions = ""
        for row in self.conn.execute("SELECT * FROM changesets WHERE graph = ? AND timestamp > ? ORDER BY timestamp DESC", (graph, timestamp)):
            if row["is_insertion"]:
                additions += f"{row['subject']} {row['predicate']} {row['object']} .\n"
            else:
                deletions += f"{row['subject']} {row['predicate']} {row['object']} .\n"
        addGraph = Graph()
        addGraph.parse(data=additions, format="turtle")
        delGraph = Graph()
        delGraph.parse(data=deletions, format="turtle")
        return g - delGraph + addGraph

    def versions(self, graph) -> list[str]:
        return [row["timestamp"] for row in self.conn.execute("SELECT DISTINCT timestamp FROM changesets WHERE graph = ?", (graph,))]

    def close(self):
        self.conn.close()

if __name__ == "__main__":
    db = DB("test.db")

    # using logical timestamps here (0, 1, 2, 3, ...). If these are
    # ommitted it defaults to the current system time.
    with db.new_changeset("my-building", 0) as cs:
        cs.load_file("https://github.com/BrickSchema/Brick/releases/download/nightly/Brick.ttl")

    with db.new_changeset("my-building", 1) as cs:
        cs.add((BLDG.vav1, A, BRICK.VAV))
        cs.add((BLDG.vav1, BRICK.feeds, BLDG.zone1))
        cs.add((BLDG.zone1, A, BRICK.HVAC_Zone))
        cs.add((BLDG.zone1, BRICK.hasPart, BLDG.room1))

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
    print(len(db.latest("my-building")))

    for logical_ts in db.versions("my-building"):
        print("LOGICAL TS:", logical_ts)
        t0 = time.time()
        print(len(db.graph_at("my-building", logical_ts)))
        print(f"Loaded graph in {time.time() - t0} seconds")

    db.close()
