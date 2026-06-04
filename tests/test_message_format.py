import json
import os
import sys
import unittest


ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from message import Message


class MessageFormatTests(unittest.TestCase):
    def test_message_contains_required_fields(self):
        message = Message(
            "ELECTION",
            sender_id=1,
            target_id=2,
            election_id="e-1",
            payload={"reason": "leader_timeout"}
        )

        data = message.to_dict()

        self.assertEqual(
            {
                "type",
                "sender_id",
                "target_id",
                "timestamp",
                "msg_id",
                "election_id",
                "payload"
            },
            set(data.keys())
        )
        self.assertEqual("ELECTION", data["type"])
        self.assertEqual(1, data["sender_id"])
        self.assertEqual(2, data["target_id"])
        self.assertEqual("e-1", data["election_id"])
        self.assertEqual({"reason": "leader_timeout"}, data["payload"])

    def test_json_roundtrip_preserves_ids(self):
        message = Message("COORDINATOR", sender_id=5, target_id=1, election_id="e-2")
        restored = Message.from_json(message.to_json())

        self.assertEqual(message.type, restored.type)
        self.assertEqual(message.sender_id, restored.sender_id)
        self.assertEqual(message.target_id, restored.target_id)
        self.assertEqual(message.msg_id, restored.msg_id)
        self.assertEqual(message.election_id, restored.election_id)

    def test_json_output_is_valid_json(self):
        message = Message("OK", sender_id=3, target_id=1)
        decoded = json.loads(message.to_json())

        self.assertEqual("OK", decoded["type"])


if __name__ == "__main__":
    unittest.main()
