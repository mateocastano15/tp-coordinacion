import os
import logging
import threading
import zlib

from common import middleware, message_protocol, fruit_item
from common.middleware.middleware_rabbitmq import PREFETCH_COUNT

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

MsgType = message_protocol.internal.MsgType


def build_data_output_exchanges():
    data_output_exchanges = []
    for i in range(AGGREGATION_AMOUNT):
        data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
        )
        data_output_exchanges.append(data_output_exchange)
    return data_output_exchanges


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [f"{SUM_PREFIX}_{i}" for i in range(SUM_AMOUNT)],
        )
        self.data_output_exchanges = build_data_output_exchanges()
        self.lock = threading.Lock()
        self.amount_by_fruit_by_client = {}
        self.records_by_client = {}
        self.finished_clients = {}
        self.processed_messages = 0

    def _aggregation_index(self, fruit):
        return zlib.crc32(fruit.encode("utf-8")) % AGGREGATION_AMOUNT

    def _pop_client(self, client_id):
        amount_by_fruit = self.amount_by_fruit_by_client.pop(client_id, {})
        records = self.records_by_client.pop(client_id, 0)
        return amount_by_fruit, records

    def _send_partial_sums(self, client_id, amount_by_fruit, data_output_exchanges):
        logging.info(f"Sending data messages")
        for final_fruit_item in amount_by_fruit.values():
            data_output_exchange = data_output_exchanges[
                self._aggregation_index(final_fruit_item.fruit)
            ]
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [
                        MsgType.DATA,
                        client_id,
                        final_fruit_item.fruit,
                        final_fruit_item.amount,
                    ]
                )
            )

    def _broadcast(self, message, data_output_exchanges):
        for data_output_exchange in data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize(message))

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
        amount_by_fruit[fruit] = amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))
        self.records_by_client[client_id] = (
            self.records_by_client.get(client_id, 0) + 1
        )

        if client_id in self.finished_clients:
            logging.info(f"Sending late data")
            amount_by_fruit, records = self._pop_client(client_id)
            self._send_partial_sums(
                client_id, amount_by_fruit, self.data_output_exchanges
            )
            self._broadcast(
                [MsgType.LATE_EOF, client_id, records], self.data_output_exchanges
            )

    def _process_eof(self, client_id, total_records):
        logging.info(f"Broadcasting EOF to sum instances")
        self.control_output_exchange.send(
            message_protocol.internal.serialize(
                [MsgType.EOF, client_id, total_records]
            )
        )

    def _forget_finished_clients(self):
        for client_id, processed_messages in list(self.finished_clients.items()):
            if self.processed_messages - processed_messages >= PREFETCH_COUNT:
                del self.finished_clients[client_id]

    def process_data_messsage(self, message, ack, nack):
        [msg_type, client_id, *payload] = message_protocol.internal.deserialize(
            message
        )
        with self.lock:
            if msg_type == MsgType.DATA:
                self._process_data(client_id, *payload)
            elif msg_type == MsgType.EOF:
                self._process_eof(client_id, *payload)
            else:
                logging.error(f"Unexpected message type: {msg_type}")
            self.processed_messages += 1
            self._forget_finished_clients()
        ack()

    def process_control_message(self, message, ack, data_output_exchanges):
        [msg_type, client_id, total_records] = message_protocol.internal.deserialize(
            message
        )
        if msg_type != MsgType.EOF:
            logging.error(f"Unexpected message type: {msg_type}")
            ack()
            return

        with self.lock:
            amount_by_fruit, records = self._pop_client(client_id)
            self.finished_clients[client_id] = self.processed_messages

        self._send_partial_sums(client_id, amount_by_fruit, data_output_exchanges)
        logging.info(f"Broadcasting EOF message")
        self._broadcast(
            [MsgType.EOF, client_id, records, total_records], data_output_exchanges
        )
        ack()

    def _consume_control_messages(self):
        control_input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_{ID}"]
        )
        data_output_exchanges = build_data_output_exchanges()
        control_input_exchange.start_consuming(
            lambda message, ack, nack: self.process_control_message(
                message, ack, data_output_exchanges
            )
        )

    def start(self):
        control_thread = threading.Thread(
            target=self._consume_control_messages, daemon=True
        )
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
