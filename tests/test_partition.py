import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import config
from message import Message
from network import NetworkBus


class PartitionTests(unittest.TestCase):
    def test_partition_blocks_cross_group_messages(self):
        original_loss = config.MESSAGE_LOSS_RATE
        original_latency = config.NETWORK_LATENCY_MS
        config.MESSAGE_LOSS_RATE = 0.0
        config.NETWORK_LATENCY_MS = 0

        try:
            network = NetworkBus()
            network.register_node(1)
            network.register_node(2)
            network.register_node(3)
            network.start_partition([[1, 2], [3]])

            network.send(Message("HEARTBEAT", sender_id=1, target_id=3))
            network.send(Message("HEARTBEAT", sender_id=1, target_id=2))

            self.assertIsNone(network.receive(3, timeout=0.01))
            self.assertIsNotNone(network.receive(2, timeout=1))
        finally:
            config.MESSAGE_LOSS_RATE = original_loss
            config.NETWORK_LATENCY_MS = original_latency

    def test_restore_partition_clears_status(self):
        network = NetworkBus()
        network.start_partition([[1], [2]])

        network.restore_partition()

        self.assertFalse(network.get_status()["partition_active"])
        self.assertEqual([], network.get_status()["partition_groups"])


if __name__ == "__main__":
    unittest.main()
