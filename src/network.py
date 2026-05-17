import random
import time
import threading
from queue import Queue
from message import Message
import config


class NetworkBus:
    """Thread-safe in-memory network bus with loss, latency and partitions."""

    def __init__(self):
        """Initialize node queues and network simulation flags."""
        self.queues = {}
        self.lock = threading.Lock()
        self.logger = None
        self.partition_active = False
        self.partition_groups = []

    def register_node(self, node_id):
        """Register or replace the receive queue for a node."""
        with self.lock:
            self.queues[node_id] = Queue()

    def unregister_node(self, node_id):
        """Remove a node from the network if it is still registered."""
        with self.lock:
            if node_id in self.queues:
                del self.queues[node_id]

    def send(self, message: Message):
        """Send a message asynchronously after applying network rules."""
        if self._is_partitioned(message.sender_id, message.target_id):
            self._log("MESSAGE_DROPPED", message)
            return

        if random.random() < config.MESSAGE_LOSS_RATE:
            self._log("MESSAGE_DROPPED", message)
            return

        self._log("MESSAGE_SENT", message)
        delay = config.NETWORK_LATENCY_MS / 1000.0

        def deliver():
            """Deliver under the bus lock so queue removal cannot race put()."""
            time.sleep(delay)
            with self.lock:
                if message.target_id in self.queues:
                    self.queues[message.target_id].put(message)

        threading.Thread(target=deliver, daemon=True).start()

    def broadcast(self, message: Message):
        """Send a copy of the message to every registered node except sender."""
        with self.lock:
            node_ids = list(self.queues.keys())

        for node_id in node_ids:
            if node_id != message.sender_id:
                msg_copy = Message(
                    msg_type=message.type,
                    sender_id=message.sender_id,
                    target_id=node_id,
                    election_id=message.election_id,
                    payload=message.payload
                )
                self.send(msg_copy)

    def receive(self, node_id, timeout=1):
        """Return the next message for a node, or None on timeout/unregister."""
        try:
            with self.lock:
                if node_id not in self.queues:
                    return None
                queue = self.queues[node_id]

            msg = queue.get(timeout=timeout)
            self._log("MESSAGE_RECEIVED", msg)
            return msg
        except Exception:
            return None

    def start_partition(self, groups):
        """Activate a network partition described by disjoint node groups."""
        with self.lock:
            self.partition_active = True
            self.partition_groups = [set(group) for group in groups]

    def end_partition(self):
        """Deactivate any active network partition."""
        with self.lock:
            self.partition_active = False
            self.partition_groups = []

    def restore_partition(self):
        """Explicit alias used by scenario-level healing code."""
        self.end_partition()

    def get_status(self):
        """Return a defensive snapshot of network simulation state."""
        with self.lock:
            return {
                "packet_loss": config.MESSAGE_LOSS_RATE,
                "latency_ms": config.NETWORK_LATENCY_MS,
                "partition_active": self.partition_active,
                "partition_groups": [
                    sorted(group)
                    for group in self.partition_groups
                ]
            }

    def _is_partitioned(self, sender_id, target_id):
        """Return True when sender and target are isolated by partition."""
        with self.lock:
            if not self.partition_active:
                return False

            sender_group = None
            target_group = None

            for index, group in enumerate(self.partition_groups):
                if sender_id in group:
                    sender_group = index
                if target_id in group:
                    target_group = index

            return (
                sender_group is not None and
                target_group is not None and
                sender_group != target_group
            )

    def _log(self, event_type, message):
        """Log message-level network events when a logger is attached."""
        if not self.logger:
            return

        self.logger.log(
            event_type,
            message.sender_id,
            {
                "type": message.type,
                "sender_id": message.sender_id,
                "target_id": message.target_id,
                "timestamp": message.timestamp,
                "msg_id": message.msg_id,
                "election_id": message.election_id,
                "payload": message.payload
            }
        )
