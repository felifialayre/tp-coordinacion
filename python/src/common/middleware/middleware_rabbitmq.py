import pika
import pika.exceptions

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


class _RabbitMQBase:
    """Lógica común de conexión, consumo y cierre para Queue y Exchange."""

    connection: pika.BlockingConnection
    channel: pika.adapters.blocking_connection.BlockingChannel
    consumer_tag: str | None
    queue_name: str

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
        self.consumer_tag = None

    def _consume(self, on_message_callback, prefetch_count=None):
        def callback(ch, method, _properties, body):

            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            return on_message_callback(body, ack, nack)

        try:
            if prefetch_count:
                self.channel.basic_qos(prefetch_count=prefetch_count)
            self.consumer_tag = self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while consuming from {self.queue_name}") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while consuming from queue '{self.queue_name}'") from e
        finally:
            self.consumer_tag = None

    def stop_consuming(self):
        if not self.consumer_tag:
            return

        try:
            self.channel.stop_consuming(consumer_tag=self.consumer_tag)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while stop consuming from '{self.queue_name}'") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while stop consuming from '{self.queue_name}'") from e

        self.consumer_tag = None

    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            # close no idempotente -> si cierro algo ya cerrado explota
            raise MessageMiddlewareCloseError("Error while closing connection") from e


class MessageMiddlewareQueueRabbitMQ(_RabbitMQBase, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._connect(host)
        self.queue_name = queue_name
        try:
            self.channel.queue_declare(queue=queue_name, durable=True)
        except pika.exceptions.AMQPError as e:
            self.connection.close()
            raise MessageMiddlewareMessageError(f"Not able to declare queue: '{queue_name}'") from e

    def start_consuming(self, on_message_callback):
        self._consume(on_message_callback, prefetch_count=1)

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
        try:
            self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
            result = self.channel.queue_declare(queue='', exclusive=True)
        except pika.exceptions.AMQPError as e:
            self.connection.close()
            raise MessageMiddlewareMessageError("Not able to declare queue") from e

        self.queue_name = result.method.queue

    def start_consuming(self, on_message_callback):
        try:
            for key in self.routing_keys:
                self.channel.queue_bind(queue=self.queue_name,
                                        exchange=self.exchange_name,
                                        routing_key=key)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError("Could not bind to routing keys") from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError("Error binding to routing keys") from e

        self._consume(on_message_callback)

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
