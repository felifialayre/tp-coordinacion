import os
import logging
import signal
import zlib

from common import middleware, message_protocol, fruit_item
from common.message_protocol.internal import Message, MessageType
from common.middleware.middleware import MessageMiddlewareCloseError

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self.multiqueue = middleware.MessageMiddlewareMultiRabbitMQ(
            host=MOM_HOST,  exchange_name=SUM_CONTROL_EXCHANGE, queue_name=INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit_by_client_id = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data")
        client_fruits = self.amount_by_fruit_by_client_id.setdefault(client_id, {})
        client_fruits[fruit] = client_fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")
        for final_fruit_item in self.amount_by_fruit_by_client_id[client_id].values():
            data_msg = Message(client_id,
                          MessageType.DATA,
                          [final_fruit_item.fruit, final_fruit_item.amount])
            # hasheo el índice con crc32 -> determinístico
            idx = zlib.crc32(final_fruit_item.fruit.encode()) % AGGREGATION_AMOUNT
            self.data_output_exchanges[idx].send(data_msg.serialize())

        logging.info(f"Broadcasting EOF message")
        eof_msg = Message(client_id, MessageType.EOF)
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(eof_msg.serialize())

        self.amount_by_fruit_by_client_id[client_id] = {}


    def process_data_messsage(self, message, ack, _nack):
        logging.info("Process message")
        msg = Message.deserialize(message)
        if msg.type == MessageType.DATA:
            self._process_data(msg.client_id, msg.fruit, msg.amount)
        else:
            # acá propagamos el eof a través del exchange
            self.multiqueue.send(message)
        ack()

    def process_eof_messsage(self, message, ack, nack):
        logging.info("Process eof")
        msg = Message.deserialize(message)
        if msg.type == MessageType.EOF: # por las dudas checkeo..
            self._process_eof(msg.client_id)
        else:
            nack()
        ack()

    def start(self):
        try:
            self.multiqueue.start_consuming(
                    message_callback_queue=self.process_data_messsage,
                    message_callback_exchange=self.process_eof_messsage
            )
        finally:
            self.multiqueue.close()
            for i in range(len(self.data_output_exchanges)):
                try:
                    self.data_output_exchanges[i].close()
                except MessageMiddlewareCloseError as e:
                    logging.error(f"Error closing exchange {i}")

    def _handle_sigterm(self, _sig, _frame):
        self.multiqueue.stop_consuming()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
