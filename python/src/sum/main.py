import os
import logging
import signal
import zlib

from common import middleware, fruit_item
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

        # cuenta cantidad de fruta por cliente, sirve para acumular
        self.amount_by_fruit_by_client_id = {}
        # cuenta la cantidad de msjs de data que se recibió por cada cliente
        self.data_messages_per_client_id = {}
        # fija la cantidad total de msjs de data total por cliente
        self.expected_per_client_id = {}
        # acumula localmente cuantos msjs de data se procesaron por cliente
        self.count_by_client_id = {}
        # registro de clientes ya flusheados para evitar envios inecesarios
        self.flushed_client_ids = set()

    def _process_data(self, client_id, fruit, amount):
        logging.info("Process data")

        msgs_count = self.data_messages_per_client_id.setdefault(client_id, 0) + 1
        self.data_messages_per_client_id[client_id] = msgs_count

        client_fruits = self.amount_by_fruit_by_client_id.setdefault(client_id, {})
        client_fruits[fruit] = (client_fruits.get(fruit, fruit_item.FruitItem(fruit, 0))
                                + fruit_item.FruitItem(fruit, int(amount)))

    def _process_eof(self, client_id):
        logging.info(f"Broadcasting data messages")
        for final_fruit_item in self.amount_by_fruit_by_client_id.get(client_id, {}).values():
            data_msg = Message(client_id,
                          MessageType.DATA,
                          [final_fruit_item.fruit, final_fruit_item.amount])
            # hasheo el nombre con crc32 -> determinístico
            idx = zlib.crc32(final_fruit_item.fruit.encode()) % AGGREGATION_AMOUNT
            self.data_output_exchanges[idx].send(data_msg.serialize())

        logging.info(f"Broadcasting EOF message")
        eof_msg = Message(client_id, MessageType.EOF)
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(eof_msg.serialize())

        self.amount_by_fruit_by_client_id.pop(client_id, None)
        self.data_messages_per_client_id.pop(client_id, None)
        self.expected_per_client_id.pop(client_id, None)
        self.count_by_client_id.pop(client_id, None)


    def process_input_messsage(self, message, ack, _nack):
        logging.info("Process input message")
        msg = Message.deserialize(message)
        if msg.type == MessageType.DATA:
            self._process_data(msg.client_id, msg.fruit, msg.amount)
            if msg.client_id in self.expected_per_client_id:
                # esto significa que esta info es delta -> aviso al control
                # es posible que con este nuevo mensaje hayamos llegado al número esperado
                self._broadcast_count(msg.client_id)
                self._check_and_flush(msg.client_id)
        elif msg.type == MessageType.EOF:
            self.expected_per_client_id[msg.client_id] = msg.payload
            self._broadcast_count(msg.client_id)
            self._check_and_flush(msg.client_id)
        ack()

    def process_control_messsage(self, message, ack, _nack):
        logging.info("Process control message")
        msg = Message.deserialize(message)
        client_id = msg.client_id

        if client_id in self.flushed_client_ids:  # ya cerré este cliente -> ignoro tardíos
            ack()
            return

        if msg.type == MessageType.CONTROL:
            first_time = client_id not in self.expected_per_client_id
            self.expected_per_client_id[client_id] = msg.payload[0]

            # cuando leo del mensaje tengo payload en str -> paso a int
            sender_counts = {}
            for sum_id, count in msg.payload[1].items():
                sender_counts[int(sum_id)] = count

            # fusiono lo que recibí en MI copia local
            self.count_by_client_id.setdefault(client_id, {}).update(sender_counts)

            if first_time:
                # me entero que llegó el eof de client_id -> aviso a control
                self._broadcast_count(client_id)

            self._check_and_flush(client_id)
        ack()

    def _broadcast_count(self, client_id):
        payload = [self.expected_per_client_id[client_id],
                   {ID: self.data_messages_per_client_id.get(client_id, 0)}]
        self.multiqueue.send(Message(client_id, MessageType.CONTROL, payload).serialize())

    def _check_and_flush(self, client_id):
        if client_id in self.flushed_client_ids:
            # llegó mi propio mensaje -> ya flushee
            return
        parcial_count = self.count_by_client_id.setdefault(client_id, {})
        parcial_count[ID] = self.data_messages_per_client_id.get(client_id, 0)
        if sum(parcial_count.values()) == self.expected_per_client_id[client_id]:
            self._process_eof(client_id)
            self.flushed_client_ids.add(client_id)


    def start(self):
        try:
            self.multiqueue.start_consuming(
                    message_callback_queue=self.process_input_messsage,
                    message_callback_exchange=self.process_control_messsage
            )
        finally:
            self.multiqueue.close()
            for i in range(len(self.data_output_exchanges)):
                try:
                    self.data_output_exchanges[i].close()
                except MessageMiddlewareCloseError as e:
                    logging.error(f"Error closing exchange {i}: {e}")

    def _handle_sigterm(self, _sig, _frame):
        self.multiqueue.stop_consuming()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
