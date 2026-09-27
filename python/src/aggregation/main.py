import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

MsgType = message_protocol.internal.MsgType


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client = {}
        self.records_by_client = {}
        self.total_records_by_client = {}
        self.sum_eofs_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        fruit_top = self.fruit_top_by_client.setdefault(client_id, [])
        for i in range(len(fruit_top)):
            if fruit_top[i].fruit == fruit:
                updated_fruit_item = fruit_top.pop(i) + fruit_item.FruitItem(
                    fruit, amount
                )
                bisect.insort(fruit_top, updated_fruit_item)
                return
        bisect.insort(fruit_top, fruit_item.FruitItem(fruit, amount))

    def _add_records(self, client_id, records):
        self.records_by_client[client_id] = (
            self.records_by_client.get(client_id, 0) + records
        )

    def _process_eof(self, client_id, records, total_records):
        logging.info("Received EOF")
        self.sum_eofs_by_client[client_id] = (
            self.sum_eofs_by_client.get(client_id, 0) + 1
        )
        self.total_records_by_client[client_id] = total_records
        self._add_records(client_id, records)
        self._send_top_if_finished(client_id)

    def _process_late_eof(self, client_id, records):
        logging.info("Received late EOF")
        self._add_records(client_id, records)
        self._send_top_if_finished(client_id)

    def _send_top_if_finished(self, client_id):
        if self.sum_eofs_by_client.get(client_id, 0) < SUM_AMOUNT:
            return
        if self.records_by_client[client_id] < self.total_records_by_client[client_id]:
            return

        logging.info("Sending top")
        self.sum_eofs_by_client.pop(client_id)
        self.records_by_client.pop(client_id)
        self.total_records_by_client.pop(client_id)
        fruit_chunk = self.fruit_top_by_client.pop(client_id, [])[-TOP_SIZE:]
        fruit_chunk.reverse()
        fruit_top = list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
        self.output_queue.send(
            message_protocol.internal.serialize([MsgType.TOP, client_id, fruit_top])
        )

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        [msg_type, client_id, *payload] = message_protocol.internal.deserialize(
            message
        )
        if msg_type == MsgType.DATA:
            self._process_data(client_id, *payload)
        elif msg_type == MsgType.EOF:
            self._process_eof(client_id, *payload)
        elif msg_type == MsgType.LATE_EOF:
            self._process_late_eof(client_id, *payload)
        else:
            logging.error(f"Unexpected message type: {msg_type}")
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()
