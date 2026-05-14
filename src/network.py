import random
import time
import threading
from queue import Queue
from message import Message
import config


class NetworkBus:
    def __init__(self):
        self.queues = {}
        self.lock = threading.Lock()

    def register_node(self, node_id):
        with self.lock:
            self.queues[node_id] = Queue()

    def unregister_node(self, node_id):
        with self.lock:
            if node_id in self.queues:
                del self.queues[node_id]

    def send(self, message: Message):
        if random.random() < config.MESSAGE_LOSS_RATE:
            return

        delay = config.NETWORK_LATENCY_MS / 1000.0

        def deliver():
            time.sleep(delay)
            with self.lock:
                if message.target_id in self.queues:
                    self.queues[message.target_id].put(message)

        threading.Thread(target=deliver, daemon=True).start()

    def broadcast(self, message: Message):
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
        try:
            with self.lock:
                if node_id not in self.queues:
                    return None
                queue = self.queues[node_id]

            msg = queue.get(timeout=timeout)
            return msg
        except Exception:
            return None