from datetime import datetime
import uuid
import sqlite3
from contextlib import contextmanager
from rdflib import Namespace
from brickschema.namespaces import BRICK, A
from brickschema import Graph

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

class Changeset(Graph):
    def __init__(self, graph_name):
        self.name = graph_name
        self.uid = uuid.uuid4()
        self.additions = []
        self.deletions = []

    def add(self, triple):
        self.additions.append(triple)

    def remove(self, triple):
        self.deletions.append(triple)

class DB:
    def __init__(self, file_name):
        self.file_name = file_name
        self.conn = sqlite3.connect(self.file_name)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.conn.execute(changeset_table_defn)
        self.conn.execute(triple_table_defn)
        self.conn.execute(triple_unique_idx)

    @contextmanager
    def new_changeset(self, graph, ts=None):
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

    def triples(self, graph):
        for row in self.conn.execute("SELECT * FROM triples WHERE graph = ?", (graph,)):
            yield row

    def latest(self, graph):
        g = Graph()
        f = ""
        for row in self.conn.execute("SELECT * FROM triples WHERE graph = ?", (graph,)):
            f += f"{row['subject']} {row['predicate']} {row['object']} .\n"
        g.parse(data=f, format="turtle")
        return g

    def graph_at(self, graph, timestamp=None):
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")
        g = self.latest(graph)

        additions = ""
        deletions = ""
        for row in self.conn.execute("SELECT * FROM changesets WHERE graph = ? AND timestamp > ?", (graph, timestamp)):
            if row["is_insertion"]:
                additions += f"{row['subject']} {row['predicate']} {row['object']} .\n"
                #g.add((row["subject"], row["predicate"], row["object"]))
            else:
                deletions += f"{row['subject']} {row['predicate']} {row['object']} .\n"
                #g.remove((row["subject"], row["predicate"], row["object"]))
        addGraph = Graph()
        addGraph.parse(data=additions, format="turtle")
        delGraph = Graph()
        delGraph.parse(data=deletions, format="turtle")
        return g - delGraph + addGraph

    def close(self):
        self.conn.close()

if __name__ == "__main__":
    db = DB("test.db")

    with db.new_changeset("abc", 1) as cs:
        cs.add((BLDG.vav1, A, BRICK.VAV))
        cs.add((BLDG.vav1, BRICK.feeds, BLDG.zone1))
        cs.add((BLDG.zone1, A, BRICK.HVAC_Zone))
        cs.add((BLDG.zone1, BRICK.hasPart, BLDG.room1))


    with db.new_changeset("abc", 2) as cs:
        cs.add((BLDG.vav2, A, BRICK.VAV))
        cs.add((BLDG.vav2, BRICK.feeds, BLDG.zone1))
        cs.add((BLDG.zone2, A, BRICK.HVAC_Zone))
        cs.remove((BLDG.zone1, BRICK.hasPart, BLDG.room1))

    with db.new_changeset("abc", 3) as cs:
        cs.add((BLDG.vav2, A, BRICK.VAV))
        cs.add((BLDG.vav2, BRICK.feeds, BLDG.zone1))
        cs.add((BLDG.zone2, A, BRICK.HVAC_Zone))
        cs.remove((BLDG.zone1, BRICK.hasPart, BLDG.room1))

    with db.new_changeset("abc", 4) as cs:
        cs.remove((BLDG.vav2, BRICK.feeds, BLDG.zone1))
        cs.add((BLDG.vav2, BRICK.feeds, BLDG.zone2))

    print("LATEST!")
    for t in db.latest("abc"):
        print(t)

    for logical_ts in range(1, 5):
        print("LOGICAL TS:", logical_ts)
        for t in db.graph_at("abc", logical_ts):
            print(t)

    db.close()
