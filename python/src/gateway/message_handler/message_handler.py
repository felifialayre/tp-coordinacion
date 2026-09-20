from common.message_protocol import internal
import uuid

class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return internal.Message(self.client_id, internal.MessageType.DATA,[fruit, amount]).serialize()

    def serialize_eof_message(self, message):
        return internal.Message(self.client_id, internal.MessageType.EOF).serialize()

    def deserialize_result_message(self, message):
        msg = internal.Message.deserialize(message)
        return msg.payload if msg.client_id == self.client_id else []
