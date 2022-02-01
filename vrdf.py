from datetime import datetime
from collections import OrderedDict
import uuid
import time
from contextlib import contextmanager
from rdflib import Namespace
from brickschema.namespaces import BRICK, A
from rdflib import Graph, ConjunctiveGraph
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

class Changeset(Graph):
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
        self.additions.extend(g.triples((None, None, None)))

    def remove(self, triple):
        self.deletions.append(triple)


class DB(ConjunctiveGraph):
    def __init__(self, file_name, *args, **kwargs):
        store = plugin.get("SQLAlchemy", Store)(identifier="my_store")
        super().__init__(store, *args, **kwargs)
        self.file_name = file_name
        self.open(f"sqlite:///{self.file_name}", create=True)

        self._precommit_hooks = OrderedDict()
        self._postcommit_hooks = OrderedDict()

        with self.conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(changeset_table_defn)

    def add_precommit_hook(self, hook):
        self._precommit_hooks[hook.__name__] = hook

    def add_postcommit_hook(self, hook):
        self._postcommit_hooks[hook.__name__] = hook

    @contextmanager
    def conn(self):
        yield self.store.engine.connect()

    @contextmanager
    def new_changeset(self, graph_name, ts=None):
        with self.conn() as conn:
            graph = self.get_context(graph_name)
            transaction_start = time.time()
            cs = Changeset(graph_name)
            yield cs
            if ts is None:
                ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")
            # delta by the user. We need to invert the changes so that they are expressed as a "backward"
            # delta. This means that we save the deletions in the changeset as "inserts", and the additions
            # as "deletions".
            if cs.deletions:
                conn.exec_driver_sql(
                    "INSERT INTO changesets VALUES (?, ?, ?, ?, ?)",
                    [
                        (str(cs.uid), ts, graph_name, True, pickle.dumps(triple))
                        for triple in cs.deletions
                    ],
                )
                for triple in cs.deletions:
                    graph.remove(triple)
            if cs.additions:
                conn.exec_driver_sql(
                    "INSERT INTO changesets VALUES (?, ?, ?, ?, ?)",
                    [
                        (str(cs.uid), ts, graph_name, False, pickle.dumps(triple))
                        for triple in cs.additions
                    ],
                )
                for triple in cs.additions:
                    graph.add(triple)
            transaction_end = time.time()
            print(f"Transaction took {transaction_end - transaction_start} seconds")
            for hook in self._precommit_hooks.values():
                hook(self)
        for hook in self._postcommit_hooks.values():
            hook(self)

    def latest(self, graph):
        return self.get_context(graph)

    def graph_at(self, graph=None, timestamp=None):
        """
        Return *copy* of the graph at the given timestamp. Chooses the most recent timestamp
        that is less than or equal to the given timestamp.
        """
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")
        g = Graph()
        if graph is not None:
            for t in self.get_context(graph).triples((None, None, None)):
                g.add(t)
        with self.conn() as conn:
            for row in conn.execute(
                "SELECT * FROM changesets WHERE graph = ? AND timestamp > ?",
                (graph, timestamp),
            ):
                triple = pickle.loads(row["triple"])
                if row["is_insertion"]:
                    g.add((triple[0], triple[1], triple[2]))
                else:
                    g.remove((triple[0], triple[1], triple[2]))
        return g


if __name__ == "__main__":
    db = DB("test.db")

    # can add precommit and postcommit hooks to implement desired functionality
    # precommit hooks are run *before* the transaction is committed but *after* all of
    # the changes have been made to the graph.
    # postcommit hooks are run *after* the transaction is committed.
    import pyshacl
    def validate(graph):
        print("Validating graph")
        valid, _, report = pyshacl.validate(graph, advanced=True, allow_warnings=True)
        assert valid, report
    db.add_postcommit_hook(validate)

    # using logical timestamps here (0, 1, 2, 3, ...). If these are
    # ommitted it defaults to the current system time.
    with db.new_changeset("my-building", 1) as cs:
        cs.add((BLDG.vav1, A, BRICK.VAV))
        cs.add((BLDG.vav1, BRICK.feeds, BLDG.zone1))
        cs.add((BLDG.zone1, A, BRICK.HVAC_Zone))
        cs.add((BLDG.zone1, BRICK.hasPart, BLDG.room1))

    with db.new_changeset("brick", 1) as cs:
        # 'cs' is a rdflib.Graph that supports queries -- updates on it
        # are buffered in the transaction and cannot be queried until
        # the transaction is committed (at the end of the context block)
        cs.load_file(
            "https://sparql.gtf.fyi/ttl/Brick1.3rc1.ttl"
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
