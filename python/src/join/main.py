import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

MsgType = message_protocol.internal.MsgType


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.partial_tops_by_client = {}
        self.received_tops_by_client = {}

    def _process_top(self, client_id, partial_top):
        partial_tops = self.partial_tops_by_client.setdefault(client_id, [])
        partial_tops.extend(
            fruit_item.FruitItem(fruit, amount) for fruit, amount in partial_top
        )
        received_tops = self.received_tops_by_client.get(client_id, 0) + 1
        self.received_tops_by_client[client_id] = received_tops
        if received_tops < AGGREGATION_AMOUNT:
            return

        logging.info("Sending final top")
        fruit_chunk = sorted(self.partial_tops_by_client.pop(client_id))[-TOP_SIZE:]
        fruit_chunk.reverse()
        del self.received_tops_by_client[client_id]
        fruit_top = [(item.fruit, item.amount) for item in fruit_chunk]
        self.output_queue.send(
            message_protocol.internal.serialize([MsgType.TOP, client_id, fruit_top])
        )

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        [msg_type, client_id, *payload] = message_protocol.internal.deserialize(
            message
        )
        if msg_type == MsgType.TOP:
            self._process_top(client_id, *payload)
        else:
            logging.error(f"Unexpected message type: {msg_type}")
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
