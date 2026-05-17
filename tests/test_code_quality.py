import contextlib
import io
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
from metrics import Metrics
from network import NetworkBus


class FakeNetwork:
    def get_status(self):
        return {
            "packet_loss": 0,
            "latency_ms": 0,
            "partition_active": False,
            "partition_groups": []
        }


class FakeMetrics:
    pass


class FakeLogger:
    def log(self, event_type, node_id, details=""):
        pass


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
        self.start_count = 0
        self.election_count = 0

    def start(self):
        self.start_count += 1

    def start_election(self):
        self.election_count += 1

    def crash(self):
        self.alive = False
        self.state = "DEAD"


class CodeQualityTests(unittest.TestCase):
    def test_cluster_start_is_idempotent(self):
        with patch("main.NetworkBus", FakeNetwork), \
             patch("main.Metrics", FakeMetrics), \
             patch("main.Logger", FakeLogger), \
             patch("main.Node", FakeNode):
            cluster = Cluster(2)

            with contextlib.redirect_stdout(io.StringIO()):
                cluster.start()
                cluster.start()

            self.assertEqual([1, 1], [node.start_count for node in cluster.nodes])
            self.assertEqual([0, 1], [node.election_count for node in cluster.nodes])

    def test_network_status_is_defensive_copy(self):
        network = NetworkBus()
        network.start_partition([[1], [2]])

        status = network.get_status()
        status["partition_groups"][0].append(99)

        self.assertEqual([[1], [2]], network.get_status()["partition_groups"])

    def test_metrics_snapshot_is_json_plain_and_stable(self):
        metrics = Metrics()
        metrics.start_election("e-1", candidate_id=1)
        metrics.log_message("ELECTION", "e-1")

        snapshot = metrics.snapshot()
        snapshot["elections"][0]["messages_by_type"]["ELECTION"] = 99

        self.assertEqual(1, metrics.snapshot()["elections"][0]["messages_by_type"]["ELECTION"])
        self.assertIsInstance(snapshot["elections"][0]["messages_by_type"], dict)


if __name__ == "__main__":
    unittest.main()
