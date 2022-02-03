from datetime import datetime
import logging
from collections import OrderedDict
import uuid
import time
from contextlib import contextmanager
from rdflib import Namespace
from brickschema.namespaces import BRICK, A
from rdflib import Graph, ConjunctiveGraph
from rdflib.graph import BatchAddGraph
from rdflib import plugin
from rdflib.store import Store
from rdflib_sqlalchemy import registerplugins
import pickle

registerplugins()
logger = logging.getLogger(__name__)

changeset_table_defn = """CREATE TABLE IF NOT EXISTS changesets (
    id TEXT,
    timestamp TIMESTAMP NOT NULL,
    graph TEXT NOT NULL,
    is_insertion BOOLEAN NOT NULL,
    triple BLOB NOT NULL
);"""
changeset_table_idx = """CREATE INDEX IF NOT EXISTS changesets_idx
                        ON changesets (graph, timestamp);"""
redo_table_defn = """CREATE TABLE IF NOT EXISTS redos (
    id TEXT,
    timestamp TIMESTAMP NOT NULL,
    graph TEXT NOT NULL,
    is_insertion BOOLEAN NOT NULL,
    triple BLOB NOT NULL
);"""

class Changeset(Graph):
    def __init__(self, graph_name):
        super().__init__()
        self.name = graph_name
        self.uid = uuid.uuid4()
        self.additions = []
        self.deletions = []

    def add(self, triple):
        self.additions.append(triple)
        super().add(triple)

    def load_file(self, filename):
        g = Graph()
        g.parse(filename, format="turtle")
        self.additions.extend(g.triples((None, None, None)))
        self += g

        # propagate namespaces
        for pfx, ns in g.namespace_manager.namespaces():
            self.bind(pfx, ns)

    def remove(self, triple):
        self.deletions.append(triple)
        super().remove(triple)


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
            #conn.execute("PRAGMA synchronous=OFF;")
            conn.execute(changeset_table_defn)
            conn.execute(changeset_table_idx)
            conn.execute(redo_table_defn)

    @property
    def latest_version(self):
        with self.conn() as conn:
            rows = conn.execute("SELECT id, timestamp from changesets "
                                "ORDER BY timestamp DESC LIMIT 1")
            res = rows.fetchone()
            return res

    def __len__(self):
        # need to override __len__ because the rdflib-sqlalchemy
        # backend doesn't support .count() for recent versions of
        # SQLAlchemy
        return len(list(self.triples((None, None, None))))

    def undo(self):
        """
        Undoes the given changeset. If no changeset is given,
        undoes the most recent changeset.
        """
        if self.latest_version is None:
            raise Exception("No changesets to undo")
        with self.conn() as conn:
            changeset_id = self.latest_version['id']
            logger.info(f"Undoing changeset {changeset_id}")
            self._graph_at(self, conn, self.latest_version["timestamp"])
            conn.execute("INSERT INTO redos SELECT * FROM changesets WHERE id = ?", (changeset_id,))
            conn.execute("DELETE FROM changesets WHERE id = ?", (changeset_id,))

    def redo(self):
        """
        Redoes the most recent changeset.
        """
        with self.conn() as conn:
            redo_record = conn.execute("SELECT * from redos "
                                        "ORDER BY timestamp ASC LIMIT 1").fetchone()
            if redo_record is None:
                raise Exception("No changesets to redo")
            changeset_id = redo_record['id']
            logger.info(f"Redoing changeset {changeset_id}")
            conn.execute("INSERT INTO changesets SELECT * FROM redos WHERE id = ?", (changeset_id,))
            conn.execute("DELETE FROM redos WHERE id = ?", (changeset_id,))
            self._graph_at(self, conn, redo_record["timestamp"])
            for row in conn.execute("SELECT * from changesets WHERE id = ?", (changeset_id,)):
                triple = pickle.loads(row["triple"])
                if row["is_insertion"]:
                    self.remove((triple[0], triple[1], triple[2]))
                else:
                    self.add((triple[0], triple[1], triple[2]))

    def versions(self, graph=None):
        """
        Return a list of all versions of the provided graph; defaults
        to the union of all graphs
        """
        with self.conn() as conn:
            if graph is None:
                rows = conn.execute("SELECT DISTINCT id, graph, timestamp from changesets "
                                    "ORDER BY timestamp DESC")
            else:
                rows = conn.execute("SELECT DISTINCT id, graph, timestamp from changesets "
                                    "WHERE graph = ? ORDER BY timestamp DESC",
                                    (graph,))
            return list(rows)

    def add_precommit_hook(self, hook):
        self._precommit_hooks[hook.__name__] = hook

    def add_postcommit_hook(self, hook):
        self._postcommit_hooks[hook.__name__] = hook

    @contextmanager
    def conn(self):
        yield self.store.engine.connect()

    @contextmanager
    def new_changeset(self, graph_name, ts=None):
        namespaces = []
        with self.conn() as conn:
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
                graph = self.get_context(graph_name)
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
                with BatchAddGraph(self.get_context(graph_name), batch_size=10000) as graph:
                    for triple in cs.additions:
                        graph.add(triple)


            # take care of precommit hooks
            transaction_end = time.time()
            for hook in self._precommit_hooks.values():
                hook(self)
            # keep track of namespaces so we can add them to the graph
            # after the commit
            namespaces.extend(cs.namespace_manager.namespaces())
            logging.info(f"Committing after {transaction_end - transaction_start} seconds")
        # update namespaces
        for pfx, ns in namespaces:
            self.bind(pfx, ns)
        for hook in self._postcommit_hooks.values():
            hook(self)
        self._latest_version = ts

    def latest(self, graph):
        return self.get_context(graph)

    def graph_at(self, timestamp=None, graph=None):
        # setup graph and bind namespaces
        g = Graph()
        for pfx, ns in self.namespace_manager.namespaces():
            g.bind(pfx, ns)
        # if graph is specified, only copy triples from that graph.
        # otherwise, copy triples from all graphs.
        if graph is not None:
            for t in self.get_context(graph).triples((None, None, None)):
                g.add(t)
        else:
            for t in self.triples((None, None, None)):
                g.add(t)
        with self.conn() as conn:
            return self._graph_at(g, conn, timestamp, graph)

    def _graph_at(self, alter_graph, conn, timestamp=None, graph=None):
        """
        Return *copy* of the graph at the given timestamp. Chooses the most recent timestamp
        that is less than or equal to the given timestamp.
        """
        if timestamp is None:
            timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S%Z")

        if graph is not None:
            rows = conn.execute(
                "SELECT * FROM changesets WHERE graph = ? AND timestamp >= ? ORDER BY timestamp DESC",
                (graph, timestamp),
            )
        else:
            rows = conn.execute(
                "SELECT * FROM changesets WHERE timestamp >= ? ORDER BY timestamp DESC",
                (timestamp,),
            )
        for row in rows:
            triple = pickle.loads(row["triple"])
            if row["is_insertion"]:
                alter_graph.add((triple[0], triple[1], triple[2]))
            else:
                alter_graph.remove((triple[0], triple[1], triple[2]))
        return alter_graph


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    db = DB("test.db")
    BLDG = Namespace("urn:bldg#")
    db.bind('bldg', BLDG)

    # can add precommit and postcommit hooks to implement desired functionality
    # precommit hooks are run *before* the transaction is committed but *after* all of
    # the changes have been made to the graph.
    # postcommit hooks are run *after* the transaction is committed.
    import pyshacl
    def validate(graph):
        logging.info("Validating graph")
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
        # are buffered in the transaction and can be queried from w/n
        # the transaction.
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
