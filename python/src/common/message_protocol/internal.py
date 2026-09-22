from enum import StrEnum

import json

class MessageType(StrEnum):
    DATA = "data"
    EOF = "eof"
    RESULT = "result"

class Message:
    def __init__(self, client_id, message_type,payload=None):
        self.client_id = client_id
        self.type = MessageType(message_type)
        self.payload = payload

    def serialize(self):
        return serialize({
            "id": self.client_id,
            "type": self.type,
            "payload": self.payload,
        })

    @classmethod
    def deserialize(cls, raw):
        d = deserialize(raw)
        return cls(d["id"], d["type"], d.get("payload"))

    @property
    def fruit(self):
        return self.payload[0]

    @property
    def amount(self):
        return self.payload[1]

def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
