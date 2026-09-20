import os
import logging

from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal import Message, MessageType

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top_by_client_id = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info("Processing data message")
        fruits = self.fruit_top_by_client_id.setdefault(client_id, {})
        fruits[fruit] = fruits.get(fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, amount)

    def _process_eof(self, client_id):
        logging.info("Received EOF")
        fruits = self.fruit_top_by_client_id[client_id]
        top = sorted(fruits.values(), reverse=True)[:TOP_SIZE]
        fruit_top = [(fi.fruit, fi.amount) for fi in top]
        self.output_queue.send(Message(client_id, MessageType.RESULT, fruit_top).serialize())
        del self.fruit_top_by_client_id[client_id]

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        msg = Message.deserialize(message)
        if msg.type == MessageType.DATA:
            self._process_data(msg.client_id, msg.fruit, msg.amount)
        else:
            self._process_eof(msg.client_id)
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
