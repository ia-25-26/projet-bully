# -*- coding: utf-8 -*-
from network import NetworkBus
from node import Node
from metrics import Metrics
from logger import Logger
import time
import config


class Cluster:
    def __init__(self, n_nodes):
        self.network = NetworkBus()
        self.metrics = Metrics()
        self.logger = Logger()

        self.nodes = [
            Node(i, self.network, self.metrics, self.logger)
            for i in range(1, n_nodes + 1)
        ]

    def start(self):
        print("🚀 Démarrage du cluster")
        for node in self.nodes:
            node.start()

    def stop(self):
        for node in self.nodes:
            node.crash()

    def get_leader(self):
        return [n.id for n in self.nodes if n.alive and n.state == "LEADER"]

    def crash_node(self, node_id):
        for node in self.nodes:
            if node.id == node_id:
                node.crash()

    def wait_for_leader(self, timeout=20):
        start = time.time()

        while time.time() - start < timeout:
            leaders = self.get_leader()

            if leaders:
                return leaders

            time.sleep(1)

        return []


def scenario_A():
    print("\n=== SCENARIO A : Cluster nominal ===")
    cluster = Cluster(5)
    cluster.start()

    leaders = cluster.wait_for_leader(timeout=30)

    print("Leader(s):", leaders)
    time.sleep(5)
    cluster.metrics.export("metrics_A.json")
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
    cluster.metrics.export("metrics_B.json")
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
    cluster.metrics.export("metrics_C.json")
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
    cluster.metrics.export("metrics_D.json")
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

    original_send = cluster.network.send

    def partitioned_send(msg):
        if (msg.sender_id in group1 and msg.target_id in group2) or \
           (msg.sender_id in group2 and msg.target_id in group1):
            return
        original_send(msg)

    cluster.network.send = partitioned_send

    time.sleep(20)

    print("Leaders après partition:", cluster.get_leader())
    cluster.metrics.export("metrics_E.json")
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
    cluster.metrics.export("metrics_F.json")
    cluster.stop()


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