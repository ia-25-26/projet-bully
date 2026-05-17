import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from message import Message
from node import Node


class FakeNetwork:
    def __init__(self):
        self.sent = []

    def register_node(self, node_id):
        pass

    def send(self, message):
        self.sent.append(message)


class DuplicateMessagesTests(unittest.TestCase):
    def test_duplicate_election_message_only_generates_one_ok(self):
        network = FakeNetwork()
        node = Node(4, network)
        node.in_election = True
        message = Message("ELECTION", sender_id=2, target_id=4, election_id="e-1")

        node.handle_message(message)
        node.handle_message(message)

        self.assertEqual(1, len(network.sent))
        self.assertEqual("OK", network.sent[0].type)
        self.assertEqual("e-1", network.sent[0].election_id)


if __name__ == "__main__":
    unittest.main()
