import json
import time
import uuid


class Message:
    """Serializable message exchanged by nodes through the simulated network."""

    def __init__(self, msg_type, sender_id, target_id=None, election_id=None, payload=None):
        """Create a message with stable IDs for tracing and deduplication."""
        self.type = msg_type
        self.sender_id = sender_id
        self.target_id = target_id
        self.timestamp = time.time()
        self.msg_id = str(uuid.uuid4())
        self.election_id = election_id or str(uuid.uuid4())
        self.payload = payload or {}

    def to_dict(self):
        """Return a JSON-ready representation of the message."""
        return {
            "type": self.type,
            "sender_id": self.sender_id,
            "target_id": self.target_id,
            "timestamp": self.timestamp,
            "msg_id": self.msg_id,
            "election_id": self.election_id,
            "payload": self.payload
        }

    def to_json(self):
        """Serialize the message to a JSON string."""
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str):
        """Deserialize a message while preserving timestamp and msg_id."""
        data = json.loads(json_str)
        msg = Message(
            msg_type=data["type"],
            sender_id=data["sender_id"],
            target_id=data["target_id"],
            election_id=data["election_id"],
            payload=data["payload"]
        )
        msg.timestamp = data["timestamp"]
        msg.msg_id = data["msg_id"]
        return msg
