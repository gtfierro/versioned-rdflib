from datetime import datetime
import uuid
import sqlite3
from contextlib import contextmanager
from rdflib import Namespace
from brickschema.namespaces import BRICK, A
from rdflib import Graph
from rdflib import plugin
from rdflib.store import Store
from rdflib_sqlalchemy import registerplugins
import pickle

registerplugins()

BLDG = Namespace("urn:bldg#")

changeset_table_defn = """CREATE TABLE IF NOT EXISTS changesets (
    id TEXT,
    timestamp TIMESTAMP NOT NULL,
    graph TEXT NOT NULL,
    is_insertion BOOLEAN NOT NULL,
    triple BLOB NOT NULL
);"""

# triple_table_defn = """CREATE TABLE IF NOT EXISTS triples (
#     id INTEGER PRIMARY KEY,
#     graph TEXT NOT NULL,
#     subject TEXT NOT NULL,
#     predicate TEXT NOT NULL,
#     object TEXT NOT NULL
# );"""
# triple_unique_idx = """CREATE UNIQUE INDEX IF NOT EXISTS triple_unique_idx
#     ON triples (graph, subject, predicate, object);"""

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
        self.store = plugin.get("SQLAlchemy", Store)(identifier="my_store")
        self.graphs = {}
        self.file_name = file_name
        Graph(self.store, identifier="__base__").open(f"sqlite:///{self.file_name}", create=True)

        # self.conn = self.store.engine.connect()
        # self.conn = sqlite3.connect(self.file_name)
        # self.conn.row_factory = sqlite3.Row
        # self.cursor = self.conn.cursor()
        # self.conn.execute(changeset_table_defn)
        # self.conn.execute(triple_table_defn)
        # self.conn.execute(triple_unique_idx)
        with self.conn() as conn:
            conn.execute(changeset_table_defn)
            # conn.execute(triple_table_defn)
            # conn.execute(triple_unique_idx)

    @contextmanager
    def conn(self):
        yield self.store.engine.connect()

    @contextmanager
    def new_changeset(self, graph_name, ts=None):
        if graph_name not in self.graphs:
            graph = Graph(self.store, identifier=graph_name)
            self.graphs[graph_name] = graph
        else:
            graph = self.graphs[graph_name]
        graph.open(f"sqlite:///{self.file_name}")
        with self.conn() as conn:
            cs = Changeset(graph_name)
            yield cs
            if ts is None:
                ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")
            # delta by the user. We need to invert the changes so that they are expressed as a "backward"
            # delta. This means that we save the deletions in the changeset as "inserts", and the additions
            # as "deletions".
            for triple in cs.deletions:
                td = pickle.dumps(triple)
                conn.execute("INSERT INTO changesets VALUES (?, ?, ?, ?, ?)",
                    (str(cs.uid), ts, graph_name, True, td))
                #conn.execute("DELETE FROM triples WHERE graph = ? AND triple = ?",
                #    (graph_name, triple[0], triple[1], triple[2]))
                graph.remove(triple)
            for triple in cs.additions:
                td = pickle.dumps(triple)
                conn.execute("INSERT INTO changesets VALUES (?, ?, ?, ?, ?)",
                    (str(cs.uid), ts, graph_name, False, td))
                #conn.execute("INSERT OR IGNORE INTO triples VALUES (NULL, ?, ?, ?, ?)",
                #    (graph_name, triple[0], triple[1], triple[2]))
                graph.add(triple)
        # graph.close()

    def triples(self, graph):
        with self.conn() as conn:
            for row in conn.execute("SELECT * FROM triples WHERE graph = ?", (graph,)):
                yield row

    def latest(self, graph):
        #return self.graphs[graph].open(f"sqlite:///{self.file_name}")
        return self.graphs[graph]
        # g = Graph()
        # for row in self.conn.execute("SELECT * FROM triples WHERE graph = ?", (graph,)):
        #     g.add((row["subject"], row["predicate"], row["object"]))
        # return g

    def graph_at(self, graph, timestamp=None):
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")
        g = self.latest(graph)
        with self.conn() as conn:
            for row in conn.execute("SELECT * FROM changesets WHERE graph = ? AND timestamp > ?", (graph, timestamp)):
                triple = pickle.loads(row["triple"])
                if row["is_insertion"]:
                    g.add((triple[0], triple[1], triple[2]))
                else:
                    g.remove((triple[0], triple[1], triple[2]))
        return g

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
