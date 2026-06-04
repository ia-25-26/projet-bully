import contextlib
import io
import os
import sys
import threading
import unittest


ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from node import Node


class FakeNetwork:
    def __init__(self):
        self.lock = threading.Lock()
        self.queues = {}
        self.unregistered = []

    def register_node(self, node_id):
        self.queues[node_id] = object()

    def unregister_node(self, node_id):
        self.unregistered.append(node_id)
        self.queues.pop(node_id, None)


class FakeMetrics:
    def __init__(self):
        self.failed = []

    def fail_election(self, election_id=None, reason="timeout"):
        self.failed.append((election_id, reason))


class CrashLeaderTests(unittest.TestCase):
    def test_crashing_leader_marks_dead_and_unregisters(self):
        network = FakeNetwork()
        node = Node(5, network)
        node.state = "LEADER"
        node.leader_id = 5

        with contextlib.redirect_stdout(io.StringIO()):
            node.crash()

        self.assertFalse(node.alive)
        self.assertEqual("DEAD", node.state)
        self.assertIsNone(node.leader_id)
        self.assertIn(5, network.unregistered)

    def test_crash_during_election_records_failed_election(self):
        network = FakeNetwork()
        metrics = FakeMetrics()
        node = Node(3, network, metrics=metrics)
        node.in_election = True
        node.current_election_id = "e-crash"

        with contextlib.redirect_stdout(io.StringIO()):
            node.crash()

        self.assertEqual([("e-crash", "Node crashed during election")], metrics.failed)


if __name__ == "__main__":
    unittest.main()
