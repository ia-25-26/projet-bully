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
    def __init__(self):
        self.restored = False
        self.broadcasts = []

    def restore_partition(self):
        self.restored = True

    def get_status(self):
        return {
            "packet_loss": 0,
            "latency_ms": 0,
            "partition_active": not self.restored,
            "partition_groups": []
        }

    def broadcast(self, message):
        self.broadcasts.append(message)


class FakeMetrics:
    def __init__(self):
        self.messages = []

    def log_message(self, msg_type, election_id=None):
        self.messages.append(msg_type)


class FakeLogger:
    def __init__(self):
        self.events = []

    def log(self, event_type, node_id, details=""):
        self.events.append(event_type)


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

    def _transition_to(self, state, reason):
        self.state = state


class PartitionRecoveryTests(unittest.TestCase):
    def test_restore_partition_heals_split_brain_to_highest_alive_id(self):
        with patch("main.NetworkBus", FakeNetwork), \
             patch("main.Metrics", FakeMetrics), \
             patch("main.Logger", FakeLogger), \
             patch("main.Node", FakeNode):
            cluster = Cluster(0)
            n1 = FakeNode(1, cluster.network)
            n2 = FakeNode(2, cluster.network)
            n5 = FakeNode(5, cluster.network)
            n6 = FakeNode(6, cluster.network)
            n1.state = "LEADER"
            n5.state = "LEADER"
            n6.alive = False
            n6.state = "DEAD"
            cluster.nodes.extend([n1, n2, n5, n6])

            leader_id = cluster.restore_partition()

            self.assertEqual(5, leader_id)
            self.assertTrue(cluster.network.restored)
            self.assertEqual([5], cluster.get_leader())
            self.assertEqual("FOLLOWER", n1.state)
            self.assertEqual("FOLLOWER", n2.state)
            self.assertEqual("LEADER", n5.state)
            self.assertEqual(5, n1.leader_id)
            self.assertEqual(5, n2.leader_id)
            self.assertIn("PARTITION_RECONCILED", cluster.logger.events)


if __name__ == "__main__":
    unittest.main()
