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
from message import Message
import config


class FakeNetwork:
    def __init__(self, node_ids):
        self.lock = threading.Lock()
        self.queues = {node_id: object() for node_id in node_ids}
        self.sent = []
        self.broadcasts = []
        self.partitioned = set()

    def register_node(self, node_id):
        self.queues[node_id] = object()

    def send(self, message):
        self.sent.append(message)

    def broadcast(self, message):
        self.broadcasts.append(message)

    def _is_partitioned(self, sender_id, target_id):
        return (sender_id, target_id) in self.partitioned


class FakeMetrics:
    def __init__(self):
        self.started = []
        self.ended = []
        self.messages = []

    def start_election(self, election_id=None, candidate_id=None):
        self.started.append((election_id, candidate_id))

    def end_election(self, election_id=None, leader_id=None):
        self.ended.append((election_id, leader_id))

    def log_message(self, msg_type, election_id=None):
        self.messages.append((msg_type, election_id))


class BullyElectionTests(unittest.TestCase):
    def test_highest_id_becomes_leader_without_higher_nodes(self):
        network = FakeNetwork([1, 2, 3])
        metrics = FakeMetrics()
        node = Node(3, network, metrics=metrics)

        with contextlib.redirect_stdout(io.StringIO()):
            node.start_election()

        self.assertEqual("LEADER", node.state)
        self.assertEqual(3, node.leader_id)
        self.assertFalse(node.in_election)
        self.assertEqual([(node.current_election_id, 3)], metrics.ended)
        self.assertEqual(1, len(network.broadcasts))
        self.assertEqual("COORDINATOR", network.broadcasts[0].type)

    def test_lower_id_sends_election_to_higher_nodes(self):
        network = FakeNetwork([1, 2, 3])
        metrics = FakeMetrics()
        node = Node(1, network, metrics=metrics)
        original_timeout = config.ELECTION_TIMEOUT
        original_wait = config.COORDINATOR_WAIT

        try:
            config.ELECTION_TIMEOUT = 0.001
            config.COORDINATOR_WAIT = 0.001
            with contextlib.redirect_stdout(io.StringIO()):
                node.start_election()
        finally:
            config.ELECTION_TIMEOUT = original_timeout
            config.COORDINATOR_WAIT = original_wait

        self.assertEqual([2, 3], [message.target_id for message in network.sent])
        self.assertTrue(all(message.type == "ELECTION" for message in network.sent))

    def test_lower_node_cannot_become_leader_while_higher_is_reachable(self):
        network = FakeNetwork([1, 2, 3])
        node = Node(2, network)
        node.in_election = True
        node.state = "CANDIDATE"
        node.current_election_id = "e-1"
        node.election_epoch = 1

        with contextlib.redirect_stdout(io.StringIO()):
            promoted = node.become_leader(election_id="e-1", election_epoch=1)

        self.assertFalse(promoted)
        self.assertEqual("CANDIDATE", node.state)
        self.assertEqual([], network.broadcasts)

    def test_lower_node_can_lead_when_higher_is_partitioned(self):
        network = FakeNetwork([1, 2, 3])
        network.partitioned.add((2, 3))
        node = Node(2, network)
        node.in_election = True
        node.state = "CANDIDATE"
        node.current_election_id = "e-1"
        node.election_epoch = 1

        with contextlib.redirect_stdout(io.StringIO()):
            promoted = node.become_leader(election_id="e-1", election_epoch=1)

        self.assertTrue(promoted)
        self.assertEqual("LEADER", node.state)
        self.assertEqual(1, len(network.broadcasts))

    def test_coordinator_cancels_stale_election_thread(self):
        network = FakeNetwork([1, 2, 3])
        node = Node(2, network)
        node.in_election = True
        node.state = "CANDIDATE"
        node.current_election_id = "e-old"
        node.election_epoch = 1

        node.handle_message(Message("COORDINATOR", sender_id=3, target_id=2, election_id="e-new"))

        with contextlib.redirect_stdout(io.StringIO()):
            promoted = node.become_leader(election_id="e-old", election_epoch=1)

        self.assertFalse(promoted)
        self.assertEqual("FOLLOWER", node.state)
        self.assertEqual(3, node.leader_id)


if __name__ == "__main__":
    unittest.main()
