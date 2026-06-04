import json
import os
import sys
import tempfile
import unittest


ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import config
from logger import Logger
from message import Message
from network import NetworkBus


class LoggingTests(unittest.TestCase):
    def setUp(self):
        self.original_verbose = config.VERBOSE_LOGGING
        self.original_loss = config.MESSAGE_LOSS_RATE
        self.original_latency = config.NETWORK_LATENCY_MS
        config.VERBOSE_LOGGING = True
        config.MESSAGE_LOSS_RATE = 0.0
        config.NETWORK_LATENCY_MS = 0

    def tearDown(self):
        config.VERBOSE_LOGGING = self.original_verbose
        config.MESSAGE_LOSS_RATE = self.original_loss
        config.NETWORK_LATENCY_MS = self.original_latency

    def read_entries(self, filename):
        with open(filename, "r") as file:
            return [json.loads(line) for line in file if line.strip()]

    def test_network_logs_message_sent_and_received(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "logs.json")
            logger = Logger(log_file)
            network = NetworkBus()
            network.logger = logger
            network.register_node(1)
            network.register_node(2)

            msg = Message("OK", sender_id=1, target_id=2)
            network.send(msg)
            received = network.receive(2, timeout=1)

            self.assertIsNotNone(received)

            events = self.read_entries(log_file)
            event_types = [entry["event_type"] for entry in events]

            self.assertIn("MESSAGE_SENT", event_types)
            self.assertIn("MESSAGE_RECEIVED", event_types)
            self.assertEqual(msg.msg_id, events[0]["details"]["msg_id"])

    def test_verbose_logging_can_be_toggled_at_runtime(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, "logs.json")
            logger = Logger(log_file)

            logger.set_verbose(False)
            logger.log("SHOULD_NOT_APPEAR", 1)

            logger.set_verbose(True)
            logger.log("SHOULD_APPEAR", 1)

            events = self.read_entries(log_file)

            self.assertEqual(1, len(events))
            self.assertEqual("SHOULD_APPEAR", events[0]["event_type"])

    def test_network_partition_status_and_filtering(self):
        network = NetworkBus()
        network.register_node(1)
        network.register_node(2)
        network.start_partition([[1], [2]])

        status = network.get_status()
        self.assertTrue(status["partition_active"])
        self.assertEqual([[1], [2]], status["partition_groups"])

        network.send(Message("OK", sender_id=1, target_id=2))
        self.assertIsNone(network.receive(2, timeout=0.01))

        network.end_partition()
        self.assertFalse(network.get_status()["partition_active"])


if __name__ == "__main__":
    unittest.main()
