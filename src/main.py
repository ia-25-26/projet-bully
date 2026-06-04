# -*- coding: utf-8 -*-
import json
import threading
import time

import config
from logger import Logger
from message import Message
from metrics import Metrics
from network import NetworkBus
from node import Node


class Cluster:
    """Owns nodes, shared services and scenario-level orchestration."""

    def __init__(self, n_nodes):
        """Create a cluster with node IDs from 1 to n_nodes."""
        self.network = NetworkBus()
        self.metrics = Metrics()
        self.logger = Logger()
        self.network.logger = self.logger
        self.lock = threading.RLock()
        self.started = False
        self.monitoring = False
        self.monitor_thread = None

        self.nodes = [
            Node(i, self.network, self.metrics, self.logger)
            for i in range(1, n_nodes + 1)
        ]

    def start(self):
        """Start all current nodes exactly once."""
        with self.lock:
            if self.started:
                return

            print("🚀 Démarrage du cluster")
            self.started = True
            nodes = list(self.nodes)

        for node in nodes:
            node.start()

        self.bootstrap_election()

    def bootstrap_election(self):
        """Start exactly one initial Bully election for a fresh cluster."""
        with self.lock:
            candidates = [node for node in self.nodes if node.alive]

        if not candidates:
            return

        starter = max(candidates, key=lambda node: node.id)
        threading.Thread(target=starter.start_election, daemon=True).start()

    def stop(self):
        """Stop monitoring and crash all known nodes."""
        with self.lock:
            self.started = False
            nodes = list(self.nodes)

        self.stop_monitoring()

        for node in nodes:
            node.crash()

    def get_leader(self):
        """Return IDs of currently alive nodes in LEADER state."""
        with self.lock:
            return [n.id for n in self.nodes if n.alive and n.state == "LEADER"]

    def add_node(self, node_id):
        """Add a new node and trigger an election if it outranks the leader."""
        with self.lock:
            if any(node.id == node_id for node in self.nodes):
                raise ValueError(f"Node {node_id} already exists")

            node = Node(node_id, self.network, self.metrics, self.logger)
            self.nodes.append(node)
            leaders = self.get_leader()
            current_leader_id = max(leaders) if leaders else None
            started = self.started

        self._activate_node(node, current_leader_id, started)

        if self.logger:
            self.logger.log("NODE_JOIN", node.id, "Node joined cluster")

        return node

    def restart_node(self, node_id):
        """Replace a crashed node with a fresh instance of the same ID."""
        with self.lock:
            node_index = None

            for index, node in enumerate(self.nodes):
                if node.id == node_id:
                    node_index = index
                    crashed = not node.alive
                    break

            if node_index is None:
                raise ValueError(f"Node {node_id} does not exist")

            if not crashed:
                raise ValueError(f"Node {node_id} is already running")

            node = Node(node_id, self.network, self.metrics, self.logger)
            self.nodes[node_index] = node
            leaders = self.get_leader()
            current_leader_id = max(leaders) if leaders else None
            started = self.started

        self._activate_node(node, current_leader_id, started)

        if self.logger:
            self.logger.log("NODE_RESTART", node.id, "Node restarted")

        return node

    def _activate_node(self, node, current_leader_id, started):
        """Start a node and settle it as follower or candidate."""
        if not started:
            return

        node.start()

        if current_leader_id is None or node.id > current_leader_id:
            threading.Thread(target=node.start_election, daemon=True).start()
        else:
            with node.lock:
                if hasattr(node, "_transition_to"):
                    node._transition_to("FOLLOWER", "Node activated under current leader")
                else:
                    node.state = "FOLLOWER"
                node.leader_id = current_leader_id
                node.missed_heartbeats = 0
                node.in_election = False
                node.got_ok = False

    def crash_node(self, node_id):
        """Crash a node by ID if it exists."""
        for node in self.nodes:
            if node.id == node_id:
                node.crash()

    def restore_partition(self):
        """Restore the network and reconcile any split brain."""
        self.network.restore_partition()

        if self.logger:
            self.logger.log("PARTITION_RESTORE", None, "Network partition restored")

        return self.reconcile_partition()

    def reconcile_partition(self):
        """Force convergence to the highest alive node after partition healing."""
        with self.lock:
            alive_nodes = [node for node in self.nodes if node.alive]

            if not alive_nodes:
                return None

            final_leader = max(alive_nodes, key=lambda node: node.id)
            final_leader_id = final_leader.id

            for node in alive_nodes:
                with node.lock:
                    if hasattr(node, "_transition_to"):
                        next_state = "LEADER" if node.id == final_leader_id else "FOLLOWER"
                        node._transition_to(next_state, "Partition reconciliation")
                    else:
                        node.state = "LEADER" if node.id == final_leader_id else "FOLLOWER"

                    node.leader_id = final_leader_id
                    node.in_election = False
                    node.got_ok = False
                    node.missed_heartbeats = 0

            if self.logger:
                self.logger.log(
                    "PARTITION_RECONCILED",
                    final_leader_id,
                    {
                        "leader_id": final_leader_id,
                        "alive_nodes": [node.id for node in alive_nodes]
                    }
                )

        final_leader.network.broadcast(Message("COORDINATOR", final_leader_id))

        if self.metrics:
            self.metrics.log_message("COORDINATOR")

        return final_leader_id

    def wait_for_leader(self, timeout=20):
        """Wait until at least one leader exists."""
        start = time.time()

        while time.time() - start < timeout:
            leaders = self.get_leader()

            if leaders:
                return leaders

            time.sleep(1)

        return []

    def wait_for_stable_leader(
        self,
        expected_id=None,
        timeout=20,
        interval=0.1,
        stable_checks=3
    ):
        """Wait for one stable leader, optionally requiring a specific ID."""
        start = time.time()
        last_leaders = None
        stable_count = 0

        while time.time() - start < timeout:
            leaders = self.get_leader()
            valid = len(leaders) == 1

            if expected_id is not None:
                valid = valid and leaders[0] == expected_id

            if valid and leaders == last_leaders:
                stable_count += 1
            elif valid:
                stable_count = 1
                last_leaders = leaders
            else:
                stable_count = 0
                last_leaders = leaders

            if stable_count >= stable_checks:
                return leaders

            time.sleep(interval)

        return []

    def start_monitoring(self, interval=None):
        """Start a daemon thread that prints live cluster snapshots."""
        with self.lock:
            if self.monitoring:
                return

            self.monitoring = True

        refresh_interval = interval or config.MONITOR_INTERVAL
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            args=(refresh_interval,),
            daemon=True
        )
        self.monitor_thread.start()

    def stop_monitoring(self):
        """Stop the live monitoring loop on its next iteration."""
        with self.lock:
            self.monitoring = False

    def _monitor_loop(self, interval):
        """Print snapshots until monitoring is disabled."""
        while True:
            with self.lock:
                if not self.monitoring:
                    return

            print(self.render_monitoring_snapshot(), flush=True)
            time.sleep(interval)

    def render_monitoring_snapshot(self):
        """Render a point-in-time view of node and network state."""
        with self.lock:
            nodes = sorted(self.nodes, key=lambda node: node.id)

            node_rows = []
            for node in nodes:
                with node.lock:
                    node_rows.append({
                        "id": node.id,
                        "state": node.state,
                        "leader_id": node.leader_id,
                        "status": "alive" if node.alive else "dead",
                        "in_election": node.in_election,
                        "missed_heartbeats": node.missed_heartbeats
                    })

        network_status = self.network.get_status()
        lines = [
            "",
            "=== LIVE CLUSTER MONITOR ===",
            "ID | STATE     | LEADER | STATUS | IN_ELECTION | MISSED_HB",
            "---+-----------+--------+--------+-------------+----------"
        ]

        for row in node_rows:
            lines.append(
                f"{row['id']:>2} | "
                f"{row['state']:<9} | "
                f"{str(row['leader_id']):<6} | "
                f"{row['status']:<6} | "
                f"{str(row['in_election']):<11} | "
                f"{row['missed_heartbeats']}"
            )

        lines.extend([
            "",
            "Network:",
            f"- packet_loss: {network_status['packet_loss']}",
            f"- latency_ms: {network_status['latency_ms']}",
            f"- partition_active: {network_status['partition_active']}",
            f"- partition_groups: {network_status['partition_groups']}"
        ])

        return "\n".join(lines)


def scenario_A():
    print("\n=== SCENARIO A : Cluster nominal ===")
    cluster = Cluster(5)
    cluster.start()

    leaders = cluster.wait_for_leader(timeout=30)

    print("Leader(s):", leaders)
    time.sleep(5)
    cluster.metrics.export("metrics_A.json", scenario_name="A")
    cluster.stop()


def scenario_B():
    print("\n=== SCENARIO B : Crash du leader ===")
    cluster = Cluster(5)
    cluster.start()

    leaders = cluster.wait_for_leader(timeout=30)

    if leaders:
        leader = leaders[0]
        print(f"💀 Crash leader {leader}")
        time.sleep(2)
        cluster.crash_node(leader)

    time.sleep(20)

    print("Nouveau leader:", cluster.get_leader())
    cluster.metrics.export("metrics_B.json", scenario_name="B")
    cluster.stop()


def scenario_C():
    print("\n=== SCENARIO C : Crash non-leader ===")
    cluster = Cluster(5)
    cluster.start()

    leaders = cluster.wait_for_leader(timeout=30)

    if not leaders:
        print("❌ Aucun leader trouvé")
        cluster.stop()
        return

    leader = leaders[0]

    for n in cluster.nodes:
        if n.alive and n.id != leader:
            print(f"💀 Crash node {n.id}")
            cluster.crash_node(n.id)
            break

    time.sleep(15)

    print("Leader reste:", cluster.get_leader())
    cluster.metrics.export("metrics_C.json", scenario_name="C")
    cluster.stop()


def scenario_D():
    print("\n=== SCENARIO D : Réseau dégradé ===")

    config.MESSAGE_LOSS_RATE = 0.3
    config.NETWORK_LATENCY_MS = 200

    cluster = Cluster(5)
    cluster.start()

    leaders = cluster.wait_for_leader(timeout=40)

    print("Leader:", leaders)
    time.sleep(5)
    cluster.metrics.export("metrics_D.json", scenario_name="D")
    cluster.stop()

    config.MESSAGE_LOSS_RATE = 0.0
    config.NETWORK_LATENCY_MS = 0


def scenario_E():
    print("\n=== SCENARIO E : Partition réseau ===")

    cluster = Cluster(5)
    cluster.start()

    cluster.wait_for_leader(timeout=30)

    print("⚠️ Simulation partition réseau")

    group1 = [1, 2]
    group2 = [3, 4, 5]

    cluster.logger.log(
        "PARTITION_START",
        None,
        {
            "group1": group1,
            "group2": group2
        }
    )

    cluster.network.start_partition([group1, group2])

    time.sleep(20)

    print("Leaders pendant partition:", cluster.get_leader())

    cluster.restore_partition()
    cluster.logger.log(
        "PARTITION_END",
        None,
        {
            "group1": group1,
            "group2": group2
        }
    )

    print("Leaders apres restauration:", cluster.get_leader())
    cluster.metrics.export("metrics_E.json", scenario_name="E")
    cluster.stop()


def scenario_F():
    print("\n=== SCENARIO F : Cascade de pannes ===")

    cluster = Cluster(5)
    cluster.start()

    cluster.wait_for_leader(timeout=30)

    for _ in range(3):
        leaders = cluster.get_leader()

        if leaders:
            leader = leaders[0]
            print(f"💀 Crash leader {leader}")
            cluster.crash_node(leader)
            time.sleep(15)

    print("Leader final:", cluster.get_leader())
    cluster.metrics.export("metrics_F.json", scenario_name="F")
    cluster.stop()


def scenario_G_scale_test(output_file="metrics_G_scale.json"):
    print("\n=== SCENARIO G : Scale test N=20 / 10 elections ===")

    saved_config = {
        "HEARTBEAT_INTERVAL": config.HEARTBEAT_INTERVAL,
        "ELECTION_TIMEOUT": config.ELECTION_TIMEOUT,
        "MAX_MISSED_HB": config.MAX_MISSED_HB,
        "COORDINATOR_WAIT": config.COORDINATOR_WAIT,
        "MESSAGE_LOSS_RATE": config.MESSAGE_LOSS_RATE,
        "NETWORK_LATENCY_MS": config.NETWORK_LATENCY_MS
    }

    config.HEARTBEAT_INTERVAL = 0.1
    config.ELECTION_TIMEOUT = 0.2
    config.MAX_MISSED_HB = 2
    config.COORDINATOR_WAIT = 3 * config.ELECTION_TIMEOUT
    config.MESSAGE_LOSS_RATE = 0.0
    config.NETWORK_LATENCY_MS = 0

    cluster = Cluster(20)
    rounds = []
    validation_errors = []

    try:
        cluster.start()

        initial_leaders = cluster.wait_for_stable_leader(
            expected_id=20,
            timeout=10,
            interval=0.05
        )
        if initial_leaders != [20]:
            validation_errors.append({
                "round": "initial",
                "expected": 20,
                "actual": initial_leaders
            })

        for election_round in range(1, 11):
            alive_ids = [node.id for node in cluster.nodes if node.alive]
            if not alive_ids:
                validation_errors.append({
                    "round": election_round,
                    "error": "No alive nodes before election"
                })
                break

            crashed_leader = max(alive_ids)
            cluster.crash_node(crashed_leader)
            expected_leader = max(
                node.id for node in cluster.nodes
                if node.alive
            )

            start = time.time()
            leaders = cluster.wait_for_stable_leader(
                expected_id=expected_leader,
                timeout=10,
                interval=0.05
            )
            convergence = time.time() - start
            valid = leaders == [expected_leader]

            if not valid:
                validation_errors.append({
                    "round": election_round,
                    "expected": expected_leader,
                    "actual": leaders
                })

            rounds.append({
                "round": election_round,
                "crashed_leader": crashed_leader,
                "expected_leader": expected_leader,
                "actual_leaders": leaders,
                "convergence_seconds": convergence,
                "valid": valid
            })

        convergence_times = [
            item["convergence_seconds"]
            for item in rounds
            if item["valid"]
        ]
        report = {
            "scenario": "G_SCALE_N20",
            "node_count": 20,
            "election_rounds": 10,
            "successful_rounds": sum(1 for item in rounds if item["valid"]),
            "failed_rounds": sum(1 for item in rounds if not item["valid"]),
            "validation_success": not validation_errors and len(rounds) == 10,
            "validation_errors": validation_errors,
            "average_convergence": (
                sum(convergence_times) / len(convergence_times)
                if convergence_times else 0
            ),
            "min_convergence": min(convergence_times) if convergence_times else 0,
            "max_convergence": max(convergence_times) if convergence_times else 0,
            "rounds": rounds,
            "metrics": cluster.metrics.snapshot(scenario_name="G_SCALE_N20")
        }

        with open(output_file, "w") as file:
            json.dump(report, file, indent=4)

        print("Scale benchmark sauvegarde dans", output_file)
        print("Validation:", "OK" if report["validation_success"] else "FAILED")
        return report
    finally:
        cluster.stop()

        config.HEARTBEAT_INTERVAL = saved_config["HEARTBEAT_INTERVAL"]
        config.ELECTION_TIMEOUT = saved_config["ELECTION_TIMEOUT"]
        config.MAX_MISSED_HB = saved_config["MAX_MISSED_HB"]
        config.COORDINATOR_WAIT = saved_config["COORDINATOR_WAIT"]
        config.MESSAGE_LOSS_RATE = saved_config["MESSAGE_LOSS_RATE"]
        config.NETWORK_LATENCY_MS = saved_config["NETWORK_LATENCY_MS"]


if __name__ == "__main__":
    scenario_A()
    time.sleep(3)

    scenario_B()
    time.sleep(3)

    scenario_C()
    time.sleep(3)

    scenario_D()
    time.sleep(3)

    scenario_E()
    time.sleep(3)

    scenario_F()
