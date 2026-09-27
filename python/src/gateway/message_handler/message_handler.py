import uuid

from common import message_protocol

MsgType = message_protocol.internal.MsgType


class MessageHandler:

    def __init__(self):
        self.client_id = uuid.uuid4().hex

    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize(
            [MsgType.DATA, self.client_id, fruit, amount]
        )

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([MsgType.EOF, self.client_id])

    def deserialize_result_message(self, message):
        [msg_type, client_id, *payload] = message_protocol.internal.deserialize(
            message
        )
        if msg_type != MsgType.TOP or client_id != self.client_id:
            return None
        [fruit_top] = payload
        return fruit_top
