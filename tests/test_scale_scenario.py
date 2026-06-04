import contextlib
import io
import os
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

import config
from main import scenario_G_scale_test


class FakeMetrics:
    def snapshot(self, scenario_name=None):
        return {
            "scenario": scenario_name,
            "total_elections": 10
        }


class FakeNode:
    def __init__(self, node_id):
        self.id = node_id
        self.alive = True

    def crash(self):
        self.alive = False


class FakeCluster:
    created_node_count = None

    def __init__(self, n_nodes):
        FakeCluster.created_node_count = n_nodes
        self.nodes = [FakeNode(node_id) for node_id in range(1, n_nodes + 1)]
        self.metrics = FakeMetrics()

    def start(self):
        pass

    def stop(self):
        pass

    def crash_node(self, node_id):
        for node in self.nodes:
            if node.id == node_id:
                node.crash()

    def wait_for_stable_leader(self, expected_id=None, timeout=20, interval=0.1, stable_checks=3):
        return [expected_id] if expected_id is not None else []


class ScaleScenarioTests(unittest.TestCase):
    def test_scale_scenario_builds_20_nodes_and_10_validated_elections(self):
        saved = (
            config.HEARTBEAT_INTERVAL,
            config.ELECTION_TIMEOUT,
            config.MAX_MISSED_HB,
            config.COORDINATOR_WAIT,
            config.MESSAGE_LOSS_RATE,
            config.NETWORK_LATENCY_MS
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = os.path.join(temp_dir, "scale.json")
            with patch("main.Cluster", FakeCluster):
                with contextlib.redirect_stdout(io.StringIO()):
                    report = scenario_G_scale_test(output_file=output_file)

            self.assertTrue(os.path.exists(output_file))

        self.assertEqual(20, FakeCluster.created_node_count)
        self.assertEqual(10, report["election_rounds"])
        self.assertEqual(10, report["successful_rounds"])
        self.assertTrue(report["validation_success"])
        self.assertEqual([], report["validation_errors"])

        self.assertEqual(saved[0], config.HEARTBEAT_INTERVAL)
        self.assertEqual(saved[1], config.ELECTION_TIMEOUT)
        self.assertEqual(saved[2], config.MAX_MISSED_HB)
        self.assertEqual(saved[3], config.COORDINATOR_WAIT)
        self.assertEqual(saved[4], config.MESSAGE_LOSS_RATE)
        self.assertEqual(saved[5], config.NETWORK_LATENCY_MS)


if __name__ == "__main__":
    unittest.main()
