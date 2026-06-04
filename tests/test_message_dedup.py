import os
import sys
import time
import unittest


ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import config
from message import Message
from node import Node


class FakeNetwork:
    def __init__(self):
        self.sent = []

    def register_node(self, node_id):
        self.node_id = node_id

    def send(self, message):
        self.sent.append(message)


class FakeMetrics:
    def __init__(self):
        self.messages = []

    def log_message(self, msg_type, election_id=None):
        self.messages.append(msg_type)


class MessageDeduplicationTests(unittest.TestCase):
    def test_duplicate_message_id_is_ignored(self):
        network = FakeNetwork()
        metrics = FakeMetrics()
        node = Node(2, network, metrics=metrics)
        node.in_election = True

        msg = Message("ELECTION", sender_id=1, target_id=2)

        node.handle_message(msg)
        node.handle_message(msg)

        self.assertEqual(["ELECTION", "OK"], metrics.messages)
        self.assertEqual(1, len(network.sent))
        self.assertEqual("OK", network.sent[0].type)

    def test_processed_message_ids_expire(self):
        network = FakeNetwork()
        node = Node(2, network)
        original_ttl = config.MESSAGE_DEDUP_TTL

        try:
            config.MESSAGE_DEDUP_TTL = 0.01
            msg = Message("OK", sender_id=3, target_id=2)

            self.assertFalse(node._is_duplicate_message(msg))
            self.assertTrue(node._is_duplicate_message(msg))

            time.sleep(config.MESSAGE_DEDUP_TTL * 2)
            other = Message("OK", sender_id=3, target_id=2)
            self.assertFalse(node._is_duplicate_message(other))

            self.assertNotIn(msg.msg_id, node.processed_msg_ids)
        finally:
            config.MESSAGE_DEDUP_TTL = original_ttl


if __name__ == "__main__":
    unittest.main()
