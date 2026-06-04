# -*- coding: utf-8 -*-
import threading
import time
import uuid
from message import Message
import config


class Node:
    """One Bully participant with heartbeat detection and election handling."""

    def __init__(self, node_id, network, metrics=None, logger=None):
        """Register a node on the network with isolated mutable state."""
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
        self.current_election_id = None
        self.election_epoch = 0
        self.processed_msg_ids = {}
        self.last_dedup_cleanup = time.time()

        self.lock = threading.RLock()
        self.network.register_node(self.id)

    def start(self):
        """Start listener and heartbeat loops as daemon threads."""
        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()

    def listen(self):
        """Receive messages until the node crashes."""
        while self.alive:
            msg = self.network.receive(self.id)
            if msg:
                self.handle_message(msg)

    def handle_message(self, msg):
        """Process one message while holding the node state lock."""
        with self.lock:
            if not self.alive:
                return

            if self._is_duplicate_message(msg):
                return

            if self.metrics:
                self.metrics.log_message(msg.type, msg.election_id)

            if msg.type == "HEARTBEAT":
                if (
                    self.leader_id is not None and
                    msg.sender_id < self.leader_id and
                    self._is_registered_reachable_node(self.leader_id)
                ):
                    return

                self.leader_id = msg.sender_id
                self.missed_heartbeats = 0

                if self.state != "LEADER":
                    self._transition_to("FOLLOWER", "Heartbeat received from leader")
                    self.in_election = False
                    self.got_ok = False

            elif msg.type == "ELECTION":
                if msg.sender_id < self.id:
                    self.network.send(Message(
                        "OK",
                        self.id,
                        msg.sender_id,
                        election_id=msg.election_id
                    ))

                    if self.metrics:
                        self.metrics.log_message("OK", msg.election_id)

                    if not self.in_election and self.state != "LEADER":
                        self.leader_id = None
                        threading.Thread(target=self.start_election, daemon=True).start()

            elif msg.type == "OK":
                if self.in_election and msg.election_id == self.current_election_id:
                    self.got_ok = True

            elif msg.type == "COORDINATOR":
                if (
                    self.leader_id is not None and
                    msg.sender_id < self.leader_id and
                    self._is_registered_reachable_node(self.leader_id)
                ):
                    return

                self.leader_id = msg.sender_id
                self.current_election_id = msg.election_id
                self.in_election = False
                self.got_ok = False
                self.missed_heartbeats = 0
                self.election_epoch += 1

                if self.id == msg.sender_id:
                    self._transition_to("LEADER", "Coordinator message matches node")
                else:
                    self._transition_to("FOLLOWER", "Coordinator message received")

    def heartbeat_loop(self):
        """Send heartbeats as leader or detect leader timeout as follower."""
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
                    if self.leader_id is None:
                        continue

                    self.missed_heartbeats += 1

                    if self.missed_heartbeats >= config.MAX_MISSED_HB:
                        if (
                            self.leader_id is not None and
                            self._is_registered_reachable_node(self.leader_id)
                        ):
                            self.missed_heartbeats = config.MAX_MISSED_HB - 1
                            continue

                        self.leader_id = None

                        if not self.in_election:
                            print(f"Node {self.id} détecte leader mort → élection")
                            threading.Thread(target=self.start_election, daemon=True).start()

    def start_election(self):
        """Run one Bully election attempt from this node."""
        with self.lock:
            if not self.alive:
                return

            if self.in_election or self.state == "LEADER":
                return

            self.in_election = True
            self.got_ok = False
            self.election_epoch += 1
            election_epoch = self.election_epoch
            self.current_election_id = str(uuid.uuid4())
            election_id = self.current_election_id
            self._transition_to("CANDIDATE", "Election started")
            self.leader_id = None

            print(f"Node {self.id} lance une élection")

            if self.metrics:
                self.metrics.start_election(
                    election_id,
                    candidate_id=self.id
                )

            if self.logger:
                self.logger.log("ELECTION_START", self.id)

            higher_nodes = self._reachable_higher_nodes()

            if not higher_nodes:
                self.become_leader(election_id=election_id, election_epoch=election_epoch)
                return

            for node_id in higher_nodes:
                self.network.send(Message(
                    "ELECTION",
                    self.id,
                    node_id,
                    election_id=election_id
                ))

                if self.metrics:
                    self.metrics.log_message("ELECTION", election_id)

        time.sleep(config.ELECTION_TIMEOUT)

        with self.lock:
            if not self.alive:
                return

            if not self._is_current_election(election_id, election_epoch):
                return

            if not self.got_ok:
                if self.logger:
                    self.logger.log(
                        "ELECTION_TIMEOUT",
                        self.id,
                        "No OK received before election timeout"
                    )

                # 🔥 FIX مهم
                time.sleep(config.ELECTION_TIMEOUT / 2)

                if self._is_current_election(election_id, election_epoch) and not self.got_ok:
                    self.become_leader(election_id=election_id, election_epoch=election_epoch)
                    return

        time.sleep(config.COORDINATOR_WAIT)

        with self.lock:
            if not self.alive:
                return

            if self._is_current_election(election_id, election_epoch):
                if self.metrics:
                    self.metrics.fail_election(
                        election_id,
                        "Coordinator not received before wait timeout"
                    )

                if self.logger:
                    self.logger.log(
                        "ELECTION_TIMEOUT",
                        self.id,
                        "Coordinator not received before wait timeout"
                    )

                self.in_election = False
                self.leader_id = None
                self.current_election_id = None
                threading.Thread(target=self.start_election, daemon=True).start()

    def become_leader(self, election_id=None, election_epoch=None):
        """Promote this node and announce it as coordinator."""
        with self.lock:
            if not self.alive:
                return False

            election_id = election_id or self.current_election_id
            if election_epoch is not None and not self._is_current_election(election_id, election_epoch):
                return False

            if self._has_reachable_higher_node():
                return False

            self._transition_to("LEADER", "Node became leader")
            self.leader_id = self.id
            self.in_election = False
            self.got_ok = False
            self.missed_heartbeats = 0
            self.current_election_id = election_id

        print(f"🔥 Node {self.id} devient LEADER")

        if self.logger:
            self.logger.log("LEADER_ELECTED", self.id, "New leader")

        self.network.broadcast(Message(
            "COORDINATOR",
            self.id,
            election_id=election_id
        ))

        if self.metrics:
            self.metrics.log_message("COORDINATOR", election_id)
            self.metrics.end_election(election_id, leader_id=self.id)

        return True

    def crash(self):
        """Stop the node, unregister it and mark any local election failed."""
        with self.lock:
            if not self.alive:
                return

            print(f"💀 Node {self.id} crash")

            self.alive = False
            if self.metrics and self.in_election:
                self.metrics.fail_election(
                    self.current_election_id,
                    "Node crashed during election"
                )

            self._transition_to("DEAD", "Node crashed")
            self.leader_id = None
            self.in_election = False
            self.got_ok = False
            self.current_election_id = None
            self.election_epoch += 1

            self.network.unregister_node(self.id)

            if self.logger:
                self.logger.log("CRASH", self.id)

    def _is_duplicate_message(self, msg):
        """Return True when msg_id was already handled by this node."""
        now = time.time()
        self._cleanup_processed_messages(now)

        if msg.msg_id in self.processed_msg_ids:
            return True

        self.processed_msg_ids[msg.msg_id] = now
        return False

    def _cleanup_processed_messages(self, now):
        """Expire old deduplication entries to keep memory bounded."""
        if now - self.last_dedup_cleanup < config.MESSAGE_DEDUP_TTL:
            return

        expires_before = now - config.MESSAGE_DEDUP_TTL
        self.processed_msg_ids = {
            msg_id: seen_at
            for msg_id, seen_at in self.processed_msg_ids.items()
            if seen_at >= expires_before
        }
        self.last_dedup_cleanup = now

    def _transition_to(self, new_state, reason):
        """Apply and log a state transition if it actually changes state."""
        old_state = self.state
        if old_state == new_state:
            return

        self.state = new_state

        if self.logger:
            self.logger.log(
                "STATE_TRANSITION",
                self.id,
                {
                    "from": old_state,
                    "to": new_state,
                    "reason": reason
                }
            )

    def _is_current_election(self, election_id, election_epoch):
        """Return True if the calling election thread is still authoritative."""
        return (
            self.alive and
            self.in_election and
            self.state == "CANDIDATE" and
            self.current_election_id == election_id and
            self.election_epoch == election_epoch
        )

    def _reachable_higher_nodes(self):
        """Return registered higher IDs that are not hidden by partition."""
        with self.network.lock:
            node_ids = list(self.network.queues.keys())

        return [
            node_id for node_id in node_ids
            if node_id > self.id and self._is_reachable_node(node_id)
        ]

    def _has_reachable_higher_node(self):
        """Check if a higher reachable node exists before self-promotion."""
        return bool(self._reachable_higher_nodes())

    def _is_reachable_node(self, node_id):
        """Return False when a network partition isolates node_id."""
        partition_check = getattr(self.network, "_is_partitioned", None)
        return not partition_check or not partition_check(self.id, node_id)

    def _is_registered_reachable_node(self, node_id):
        """Return True only if node_id is still registered and reachable."""
        with self.network.lock:
            registered = node_id in self.network.queues

        return registered and self._is_reachable_node(node_id)
