import pika
import pika.exceptions

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
    MessageMiddleware
)


class _RabbitMQBase:
    """Lógica común de conexión, declaración, consumo y cierre."""

    connection: pika.BlockingConnection
    channel: pika.adapters.blocking_connection.BlockingChannel
    consumer_tags: list[str]

    def _connect(self, host):
        connection = None
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host))
            channel = connection.channel()
        except pika.exceptions.AMQPConnectionError as e:
            if connection and connection.is_open:
                connection.close()
            raise MessageMiddlewareDisconnectedError(f"Not able to connect to {host}") from e

        self.connection = connection
        self.channel = channel
        self.consumer_tags = []

    def _declare_queue(self, queue_name, durable=True):
        try:
            self.channel.queue_declare(queue=queue_name, durable=durable)
        except pika.exceptions.AMQPError as e:
            self.connection.close()
            raise MessageMiddlewareMessageError(f"Not able to declare queue: '{queue_name}'") from e
        return queue_name

    def _declare_exchange_queue(self, exchange_name, exchange_type='direct'):
        try:
            self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)
            result = self.channel.queue_declare(queue='', exclusive=True)
        except pika.exceptions.AMQPError as e:
            self.connection.close()
            raise MessageMiddlewareMessageError("Not able to declare queue") from e
        return result.method.queue

    def _bind(self, queue_name, exchange_name, routing_keys):
        try:
            for key in routing_keys:
                self.channel.queue_bind(queue=queue_name,
                                        exchange=exchange_name,
                                        routing_key=key)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Could not bind to routing keys") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error binding to routing keys") from e

    def _register_consumer(self, queue_name, on_message_callback, prefetch_count=None):
        def callback(ch, method, _properties, body):

            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            return on_message_callback(body, ack, nack)

        try:
            if prefetch_count:
                self.channel.basic_qos(prefetch_count=prefetch_count)
            tag = self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
            self.consumer_tags.append(tag)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while consuming from {queue_name}") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while consuming from queue '{queue_name}'") from e

    def _start_consuming(self):
        try:
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Connection lost while consuming") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error while consuming") from e
        finally:
            self.consumer_tags = []

    def stop_consuming(self):
        if not self.consumer_tags:
            return

        try:
            for tag in self.consumer_tags:
                self.channel.stop_consuming(consumer_tag=tag)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Connection lost while stop consuming") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error while stop consuming") from e

        self.consumer_tags = []

    def close(self):
        if not self.connection.is_open:
            return
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError("Error while closing connection") from e


class MessageMiddlewareQueueRabbitMQ(_RabbitMQBase, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._connect(host)
        self.queue_name = self._declare_queue(queue_name, durable=True)

    def start_consuming(self, on_message_callback):
        self._register_consumer(self.queue_name, on_message_callback, prefetch_count=1)
        self._start_consuming()

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                body=message,
                routing_key=self.queue_name,
                properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while sending to '{self.queue_name}'") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while sending to '{self.queue_name}'") from e


class MessageMiddlewareExchangeRabbitMQ(_RabbitMQBase, MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self._connect(host)
        self.routing_keys = routing_keys
        self.exchange_name = exchange_name
        self.queue_name = self._declare_exchange_queue(exchange_name, exchange_type='direct')

    def start_consuming(self, on_message_callback):
        self._bind(self.queue_name, self.exchange_name, self.routing_keys)
        self._register_consumer(self.queue_name, on_message_callback)
        self._start_consuming()

    def send(self, message):
        try:
            for key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    body=message,
                    routing_key=key
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while sending to '{self.queue_name}'") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while sending to '{self.queue_name}'") from e

class MessageMiddlewareMultiRabbitMQ(_RabbitMQBase, MessageMiddleware):
    """
    Esta clase la creo para poder aprovechar las propiedades de un FIFO para
    poder consumir tanto de un exchange como de una queue re-utilizando una
    misma conexión. En principio por la naturaleza del problema a solucionar
    propongo la queue como solo de lectura pero con pocos cambios en la firma
    de send podría expandirse a lectura y escritura en la queue.
    """
    def __init__(self, host, exchange_name=None, routing_keys=None, queue_name=None):
        self._connect(host)
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys if routing_keys is not None else ['']
        self.queue_name = queue_name

    def start_consuming(self, message_callback_queue, message_callback_exchange):
        if self.exchange_name:
            self.exchange_queue_name = self._declare_exchange_queue(self.exchange_name, exchange_type='fanout')
            self._bind(self.exchange_queue_name, self.exchange_name, self.routing_keys)
            self._register_consumer(self.exchange_queue_name, message_callback_exchange)
        self._declare_queue(self.queue_name, durable=True)
        self._register_consumer(self.queue_name, message_callback_queue, prefetch_count=1)
        self._start_consuming()

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange=self.exchange_name,
                body=message,
                routing_key=''
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while sending to '{self.queue_name}'") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while sending to '{self.queue_name}'") from e