import json
import threading
import time
from collections import defaultdict


ELECTION_MESSAGE_TYPES = {"ELECTION", "OK", "COORDINATOR"}


class Metrics:
    """Thread-safe collector for message counts and election convergence."""

    def __init__(self, scenario_name=None):
        """Initialize an empty metrics snapshot for one scenario."""
        self.scenario_name = scenario_name
        self.message_count = defaultdict(int)
        self.elections = {}
        self.election_order = []
        self.lock = threading.RLock()

    def log_message(self, msg_type, election_id=None):
        """Count a message globally and, when possible, for its election."""
        with self.lock:
            self.message_count[msg_type] += 1

            if election_id and msg_type in ELECTION_MESSAGE_TYPES:
                election = self.elections.get(election_id)
                if election:
                    election["messages_total"] += 1
                    election["messages_by_type"][msg_type] += 1

    def start_election(self, election_id=None, candidate_id=None):
        """Start tracking an election and return its stable election_id."""
        with self.lock:
            election_id = election_id or f"election-{len(self.election_order) + 1}"

            if election_id in self.elections:
                return election_id

            election = {
                "election_id": election_id,
                "candidate_id": candidate_id,
                "leader_id": None,
                "status": "running",
                "success": False,
                "started_at": time.time(),
                "ended_at": None,
                "duration": None,
                "messages_total": 0,
                "messages_by_type": defaultdict(int),
                "failure_reason": None
            }
            self.elections[election_id] = election
            self.election_order.append(election_id)
            return election_id

    def end_election(self, election_id=None, leader_id=None):
        """Mark a running election as successful and store convergence time."""
        with self.lock:
            election = self._get_election(election_id)
            if not election or election["status"] != "running":
                return

            now = time.time()
            election["status"] = "success"
            election["success"] = True
            election["leader_id"] = leader_id
            election["ended_at"] = now
            election["duration"] = now - election["started_at"]

    def fail_election(self, election_id=None, reason="timeout"):
        """Mark a running election as failed with a reason."""
        with self.lock:
            election = self._get_election(election_id)
            if not election or election["status"] != "running":
                return

            now = time.time()
            election["status"] = "failed"
            election["success"] = False
            election["ended_at"] = now
            election["duration"] = now - election["started_at"]
            election["failure_reason"] = reason

    def snapshot(self, scenario_name=None):
        """Return a JSON-serializable metrics snapshot."""
        with self.lock:
            elections = [
                self._serialize_election(self.elections[election_id])
                for election_id in self.election_order
            ]
            total_elections = len(elections)
            successful = [
                election for election in elections
                if election["status"] == "success"
            ]
            failed = [
                election for election in elections
                if election["status"] == "failed"
            ]
            successful_durations = [
                election["duration"] for election in successful
                if election["duration"] is not None
            ]

            return {
                "scenario": scenario_name or self.scenario_name,
                "message_count": dict(self.message_count),
                "total_elections": total_elections,
                "successful_elections": len(successful),
                "failed_elections": len(failed),
                "running_elections": total_elections - len(successful) - len(failed),
                "election_success_rate": (
                    len(successful) / total_elections
                    if total_elections else 0
                ),
                "average_convergence": (
                    sum(successful_durations) / len(successful_durations)
                    if successful_durations else 0
                ),
                "elections": elections
            }

    def export(self, filename="metrics.json", scenario_name=None):
        """Write the current metrics snapshot as formatted JSON."""
        data = self.snapshot(scenario_name=scenario_name)

        with open(filename, "w") as file:
            json.dump(data, file, indent=4)

        print("Metrics sauvegardees dans", filename)

    def _get_election(self, election_id):
        """Find an election by ID or the latest running election."""
        if election_id:
            return self.elections.get(election_id)

        for current_id in reversed(self.election_order):
            election = self.elections[current_id]
            if election["status"] == "running":
                return election

        return None

    def _serialize_election(self, election):
        """Convert internal defaultdict fields to plain dicts."""
        serialized = dict(election)
        serialized["messages_by_type"] = dict(election["messages_by_type"])
        return serialized
