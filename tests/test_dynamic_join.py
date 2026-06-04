import os
import sys
import threading
import time
import unittest
from unittest.mock import patch


ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from main import Cluster


class FakeNetwork:
    def __init__(self):
        self.status = {
            "packet_loss": 0.25,
            "latency_ms": 150,
            "partition_active": True,
            "partition_groups": [[1, 2], [3]]
        }
        self.broadcasts = []

    def get_status(self):
        return self.status

    def restore_partition(self):
        self.status["partition_active"] = False
        self.status["partition_groups"] = []

    def broadcast(self, message):
        self.broadcasts.append(message)


class FakeMetrics:
    def __init__(self):
        self.messages = []

    def log_message(self, msg_type, election_id=None):
        self.messages.append((msg_type, election_id))


class FakeLogger:
    def __init__(self):
        self.entries = []

    def log(self, event_type, node_id, details=""):
        self.entries.append((event_type, node_id, details))


class FakeNode:
    def __init__(self, node_id, network, metrics=None, logger=None):
        self.id = node_id
        self.network = network
        self.metrics = metrics
        self.logger = logger
        self.state = "FOLLOWER"
        self.leader_id = None
        self.alive = True
        self.missed_heartbeats = 0
        self.in_election = False
        self.got_ok = False
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


def wait_until(predicate, timeout=1):
    start = time.time()
    while time.time() - start < timeout:
        if predicate():
            return True
        time.sleep(0.01)
    return False


class DynamicJoinTests(unittest.TestCase):
    def build_cluster(self):
        patches = [
            patch("main.NetworkBus", FakeNetwork),
            patch("main.Metrics", FakeMetrics),
            patch("main.Logger", FakeLogger),
            patch("main.Node", FakeNode),
        ]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)
        return Cluster(0)

    def test_lower_id_joining_running_cluster_becomes_follower(self):
        cluster = self.build_cluster()
        leader = FakeNode(5, cluster.network)
        leader.state = "LEADER"
        cluster.nodes.append(leader)
        cluster.started = True

        joined = cluster.add_node(3)

        self.assertTrue(joined.started)
        self.assertFalse(joined.election_started)
        self.assertEqual("FOLLOWER", joined.state)
        self.assertEqual(5, joined.leader_id)

    def test_higher_id_joining_running_cluster_starts_election(self):
        cluster = self.build_cluster()
        leader = FakeNode(5, cluster.network)
        leader.state = "LEADER"
        cluster.nodes.append(leader)
        cluster.started = True

        joined = cluster.add_node(6)

        self.assertTrue(joined.started)
        self.assertTrue(wait_until(lambda: joined.election_started))

    def test_duplicate_node_id_is_rejected(self):
        cluster = self.build_cluster()
        cluster.nodes.append(FakeNode(2, cluster.network))

        with self.assertRaises(ValueError):
            cluster.add_node(2)

    def test_lower_id_crashed_node_restarts_as_follower(self):
        cluster = self.build_cluster()
        crashed = FakeNode(3, cluster.network)
        crashed.crash()
        leader = FakeNode(5, cluster.network)
        leader.state = "LEADER"
        cluster.nodes.extend([crashed, leader])
        cluster.started = True

        restarted = cluster.restart_node(3)

        self.assertIs(cluster.nodes[0], restarted)
        self.assertTrue(restarted.started)
        self.assertFalse(restarted.election_started)
        self.assertEqual("FOLLOWER", restarted.state)
        self.assertEqual(5, restarted.leader_id)

    def test_higher_id_crashed_node_restarts_and_starts_election(self):
        cluster = self.build_cluster()
        leader = FakeNode(5, cluster.network)
        leader.state = "LEADER"
        crashed = FakeNode(6, cluster.network)
        crashed.crash()
        cluster.nodes.extend([leader, crashed])
        cluster.started = True

        restarted = cluster.restart_node(6)

        self.assertIs(cluster.nodes[1], restarted)
        self.assertTrue(restarted.started)
        self.assertTrue(wait_until(lambda: restarted.election_started))

    def test_running_node_cannot_be_restarted(self):
        cluster = self.build_cluster()
        cluster.nodes.append(FakeNode(2, cluster.network))

        with self.assertRaises(ValueError):
            cluster.restart_node(2)

    def test_unknown_node_cannot_be_restarted(self):
        cluster = self.build_cluster()

        with self.assertRaises(ValueError):
            cluster.restart_node(99)

    def test_monitoring_snapshot_shows_nodes_and_network(self):
        cluster = self.build_cluster()
        leader = FakeNode(3, cluster.network)
        leader.state = "LEADER"
        leader.leader_id = 3
        follower = FakeNode(1, cluster.network)
        follower.leader_id = 3
        follower.missed_heartbeats = 2
        follower.in_election = True
        follower.crash()
        cluster.nodes.extend([leader, follower])

        snapshot = cluster.render_monitoring_snapshot()

        self.assertIn("LIVE CLUSTER MONITOR", snapshot)
        self.assertIn("ID | STATE", snapshot)
        self.assertIn(" 1 | DEAD", snapshot)
        self.assertIn(" 3 | LEADER", snapshot)
        self.assertIn("packet_loss: 0.25", snapshot)
        self.assertIn("latency_ms: 150", snapshot)
        self.assertIn("partition_active: True", snapshot)

    def test_restore_partition_reconciles_split_brain_to_highest_alive_id(self):
        cluster = self.build_cluster()
        leader_low = FakeNode(2, cluster.network)
        leader_low.state = "LEADER"
        leader_low.leader_id = 2
        leader_high = FakeNode(5, cluster.network)
        leader_high.state = "LEADER"
        leader_high.leader_id = 5
        follower = FakeNode(3, cluster.network)
        dead_highest = FakeNode(6, cluster.network)
        dead_highest.crash()
        cluster.nodes.extend([leader_low, leader_high, follower, dead_highest])

        final_leader_id = cluster.restore_partition()

        self.assertEqual(5, final_leader_id)
        self.assertFalse(cluster.network.get_status()["partition_active"])
        self.assertEqual([5], cluster.get_leader())
        self.assertEqual(5, leader_low.leader_id)
        self.assertEqual("FOLLOWER", leader_low.state)
        self.assertEqual(5, follower.leader_id)
        self.assertEqual("FOLLOWER", follower.state)
        self.assertEqual("DEAD", dead_highest.state)
        self.assertEqual(1, len(cluster.network.broadcasts))
        self.assertEqual("COORDINATOR", cluster.network.broadcasts[0].type)


if __name__ == "__main__":
    unittest.main()
