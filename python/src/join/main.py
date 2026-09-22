import os
import logging

from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal import Message, MessageType

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.tops_per_client_id = {}
        self.amount_by_fruit_by_client_id = {}

    def process_messsage(self, message, ack, nack):
        logging.info("Received message")
        msg = Message.deserialize(message)
        if msg.type == MessageType.RESULT:
            self._process_result(msg.client_id, msg.payload)
            ack()
        else:
            nack()

    def _process_result(self, client_id, parcial_top):
        fruits = self.amount_by_fruit_by_client_id.setdefault(client_id, {})
        for fruit, amount in parcial_top:
            # no debería "ya estar" la fruta
            # porque los aggregator no tienen intersección de frutas
            fruits[fruit] = (fruits.get(fruit, fruit_item.FruitItem(fruit, 0))
                             + fruit_item.FruitItem(fruit, amount))
        logging.info("Received parcial top")
        top_count = self.tops_per_client_id.get(client_id, 0) + 1
        self.tops_per_client_id[client_id] = top_count

        if top_count < AGGREGATION_AMOUNT:
            # faltan tops parciales
            return

        self._send_final_top(client_id)

    def _send_final_top(self, client_id):
        # vuelvo a ordenar el merge de todos los tops
        # no estoy aprovechando que sé que están ordenados?

        fruits = self.amount_by_fruit_by_client_id.get(client_id, {})
        top = sorted(fruits.values(), reverse=True)[:TOP_SIZE]
        final_top = [(fi.fruit, fi.amount) for fi in top]
        self.output_queue.send(Message(client_id, MessageType.RESULT, final_top).serialize())

        self.amount_by_fruit_by_client_id.pop(client_id, None)
        self.tops_per_client_id.pop(client_id, None)

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
