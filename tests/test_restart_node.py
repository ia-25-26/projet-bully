import os
import sys
import threading
import unittest
from unittest.mock import patch


ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from main import Cluster


class FakeNetwork:
    def get_status(self):
        return {
            "packet_loss": 0,
            "latency_ms": 0,
            "partition_active": False,
            "partition_groups": []
        }


class FakeMetrics:
    def log_message(self, msg_type, election_id=None):
        pass


class FakeLogger:
    def __init__(self):
        self.entries = []

    def log(self, event_type, node_id, details=""):
        self.entries.append((event_type, node_id, details))


class FakeNode:
    def __init__(self, node_id, network, metrics=None, logger=None):
        self.id = node_id
        self.network = network
        self.alive = True
        self.state = "FOLLOWER"
        self.leader_id = None
        self.in_election = False
        self.got_ok = False
        self.missed_heartbeats = 0
        self.lock = threading.RLock()
        self.started = False
        self.election_started = False

    def start(self):
        self.started = True

    def start_election(self):
        self.election_started = True

    def crash(self):
        self.alive = False
        self.state = "DEAD"


class RestartNodeTests(unittest.TestCase):
    def test_restart_crashed_high_id_starts_election(self):
        with patch("main.NetworkBus", FakeNetwork), \
             patch("main.Metrics", FakeMetrics), \
             patch("main.Logger", FakeLogger), \
             patch("main.Node", FakeNode):
            cluster = Cluster(0)
            leader = FakeNode(4, cluster.network)
            leader.state = "LEADER"
            crashed = FakeNode(5, cluster.network)
            crashed.crash()
            cluster.nodes.extend([leader, crashed])
            cluster.started = True

            restarted = cluster.restart_node(5)

            self.assertTrue(restarted.started)
            self.assertTrue(restarted.election_started)
            self.assertEqual("NODE_RESTART", cluster.logger.entries[-1][0])


if __name__ == "__main__":
    unittest.main()
