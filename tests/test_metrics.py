import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from metrics import Metrics


class MetricsTests(unittest.TestCase):
    def test_success_failed_rate_messages_and_average_convergence(self):
        metrics = Metrics(scenario_name="unit")

        with patch("metrics.time.time", side_effect=[10.0, 13.0, 20.0, 25.0]):
            metrics.start_election("e-1", candidate_id=1)
            metrics.log_message("ELECTION", "e-1")
            metrics.log_message("OK", "e-1")
            metrics.log_message("HEARTBEAT")
            metrics.end_election("e-1", leader_id=3)

            metrics.start_election("e-2", candidate_id=2)
            metrics.log_message("ELECTION", "e-2")
            metrics.fail_election("e-2", "timeout")

        snapshot = metrics.snapshot()

        self.assertEqual("unit", snapshot["scenario"])
        self.assertEqual(2, snapshot["total_elections"])
        self.assertEqual(1, snapshot["successful_elections"])
        self.assertEqual(1, snapshot["failed_elections"])
        self.assertEqual(0.5, snapshot["election_success_rate"])
        self.assertEqual(3.0, snapshot["average_convergence"])
        self.assertEqual(1, snapshot["message_count"]["HEARTBEAT"])
        self.assertEqual(2, snapshot["elections"][0]["messages_total"])
        self.assertEqual({"ELECTION": 1, "OK": 1}, snapshot["elections"][0]["messages_by_type"])
        self.assertEqual("timeout", snapshot["elections"][1]["failure_reason"])

    def test_export_writes_clean_json(self):
        metrics = Metrics()
        metrics.start_election("e-1", candidate_id=1)
        metrics.end_election("e-1", leader_id=1)

        with tempfile.TemporaryDirectory() as temp_dir:
            filename = os.path.join(temp_dir, "metrics.json")
            metrics.export(filename, scenario_name="A")

            with open(filename, "r") as file:
                data = json.load(file)

        self.assertEqual("A", data["scenario"])
        self.assertIn("total_elections", data)
        self.assertIn("election_success_rate", data)
        self.assertIn("average_convergence", data)
        self.assertIn("elections", data)


if __name__ == "__main__":
    unittest.main()
