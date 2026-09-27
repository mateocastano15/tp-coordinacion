import json

class MsgType:
    DATA = 1
    EOF = 2
    TOP = 3
    LATE_EOF = 4


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))
