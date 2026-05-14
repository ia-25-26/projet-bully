# -*- coding: utf-8 -*-
import threading
import time
from message import Message
import config


class Node:
    def __init__(self, node_id, network, metrics=None, logger=None):
        self.id = node_id
        self.network = network
        self.metrics = metrics
        self.logger = logger

        self.state = "FOLLOWER"
        self.leader_id = None

        self.alive = True
        self.missed_heartbeats = 0
        self.in_election = False
        self.got_ok = False

        self.lock = threading.RLock()
        self.network.register_node(self.id)

    def start(self):
        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()

    def listen(self):
        while self.alive:
            msg = self.network.receive(self.id)
            if msg:
                self.handle_message(msg)

    def handle_message(self, msg):
        with self.lock:
            if not self.alive:
                return

            if self.metrics:
                self.metrics.log_message(msg.type)

            if msg.type == "HEARTBEAT":
                self.leader_id = msg.sender_id
                self.missed_heartbeats = 0

                if self.state != "LEADER":
                    self.state = "FOLLOWER"
                    self.in_election = False
                    self.got_ok = False

            elif msg.type == "ELECTION":
                if msg.sender_id < self.id:
                    self.network.send(Message("OK", self.id, msg.sender_id))

                    if self.metrics:
                        self.metrics.log_message("OK")

                    if not self.in_election and self.state != "LEADER":
                        self.leader_id = None
                        threading.Thread(target=self.start_election, daemon=True).start()

            elif msg.type == "OK":
                self.got_ok = True

            elif msg.type == "COORDINATOR":
                self.leader_id = msg.sender_id
                self.in_election = False
                self.got_ok = False
                self.missed_heartbeats = 0

                if self.id == msg.sender_id:
                    self.state = "LEADER"
                else:
                    self.state = "FOLLOWER"

    def heartbeat_loop(self):
        while self.alive:
            time.sleep(config.HEARTBEAT_INTERVAL)

            with self.lock:
                if not self.alive:
                    return

                if self.state == "LEADER":
                    self.network.broadcast(Message("HEARTBEAT", self.id))

                    if self.metrics:
                        self.metrics.log_message("HEARTBEAT")

                else:
                    self.missed_heartbeats += 1

                    if self.missed_heartbeats >= config.MAX_MISSED_HB:
                        self.leader_id = None

                        if not self.in_election:
                            print(f"Node {self.id} détecte leader mort → élection")
                            threading.Thread(target=self.start_election, daemon=True).start()

    def start_election(self):
        with self.lock:
            if not self.alive:
                return

            if self.in_election or self.state == "LEADER":
                return

            self.in_election = True
            self.got_ok = False
            self.state = "CANDIDATE"
            self.leader_id = None

            print(f"Node {self.id} lance une élection")

            if self.metrics:
                self.metrics.start_election()

            with self.network.lock:
                higher_nodes = [
                    n for n in list(self.network.queues.keys())
                    if n > self.id
                ]

            if not higher_nodes:
                self.become_leader()
                return

            for node_id in higher_nodes:
                self.network.send(Message("ELECTION", self.id, node_id))

                if self.metrics:
                    self.metrics.log_message("ELECTION")

        time.sleep(config.ELECTION_TIMEOUT)

        with self.lock:
            if not self.alive:
                return

            if self.state != "CANDIDATE":
                return

            if not self.got_ok:
                # 🔥 FIX مهم
                time.sleep(config.ELECTION_TIMEOUT / 2)

                if self.state == "CANDIDATE" and not self.got_ok:
                    self.become_leader()
                    return

        time.sleep(config.COORDINATOR_WAIT)

        with self.lock:
            if not self.alive:
                return

            if self.state == "CANDIDATE":
                self.in_election = False
                self.leader_id = None
                threading.Thread(target=self.start_election, daemon=True).start()

    def become_leader(self):
        if not self.alive:
            return

        self.state = "LEADER"
        self.leader_id = self.id
        self.in_election = False
        self.got_ok = False
        self.missed_heartbeats = 0

        print(f"🔥 Node {self.id} devient LEADER")

        if self.metrics:
            self.metrics.end_election()

        if self.logger:
            self.logger.log("LEADER_ELECTED", self.id, "New leader")

        self.network.broadcast(Message("COORDINATOR", self.id))

        if self.metrics:
            self.metrics.log_message("COORDINATOR")

    def crash(self):
        with self.lock:
            if not self.alive:
                return

            print(f"💀 Node {self.id} crash")

            self.alive = False
            self.state = "DEAD"
            self.leader_id = None
            self.in_election = False
            self.got_ok = False

            self.network.unregister_node(self.id)

            if self.logger:
                self.logger.log("CRASH", self.id)