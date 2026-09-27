import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)

PREFETCH_COUNT = 1
EXCHANGE_TYPE = "direct"


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._queue_name = queue_name
        self._connection = None
        self._channel = None
        self._consumer_tag = None

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self._channel = self._connection.channel()
            self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self._channel.queue_declare(queue=queue_name, durable=True)
        except (pika.exceptions.AMQPConnectionError, OSError) as e:
            self._close_quietly()
            raise MessageMiddlewareDisconnectedError(
                f"No se pudo establecer la conexión con el middleware en '{host}': {e}"
            ) from e
        except Exception as e:
            self._close_quietly()
            raise MessageMiddlewareMessageError(
                f"Error inicializando la cola '{queue_name}' en '{host}': {e}"
            ) from e

    def start_consuming(self, on_message_callback):
        self._assert_connected()

        if self._consumer_tag is not None:
            raise MessageMiddlewareMessageError(
                "Ya hay un consumo activo sobre esta instancia del middleware"
            )

        def _on_delivery(channel, method, properties, body):
            delivery_tag = method.delivery_tag

            def ack():
                channel.basic_ack(delivery_tag=delivery_tag)

            def nack():
                channel.basic_reject(delivery_tag=delivery_tag, requeue=True)

            on_message_callback(body, ack, nack)

        try:
            self._consumer_tag = self._channel.basic_consume(
                queue=self._queue_name, on_message_callback=_on_delivery
            )
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Se perdió la conexión con el middleware mientras se consumía: {e}"
            ) from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(
                f"Error consumiendo mensajes de la cola '{self._queue_name}': {e}"
            ) from e
        finally:
            self._consumer_tag = None

    def stop_consuming(self):
        if self._consumer_tag is None:
            return

        self._assert_connected()

        try:
            self._connection.add_callback_threadsafe(self._stop_consuming_now)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Se perdió la conexión con el middleware al detener el consumo: {e}"
            ) from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(
                f"Error deteniendo el consumo de la cola '{self._queue_name}': {e}"
            ) from e

    def _stop_consuming_now(self):
        if self._consumer_tag is not None:
            self._channel.stop_consuming(self._consumer_tag)

    def send(self, message):
        self._assert_connected()

        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Se perdió la conexión con el middleware al enviar el mensaje: {e}"
            ) from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(
                f"Error enviando un mensaje a la cola '{self._queue_name}': {e}"
            ) from e

    def close(self):
        if self._connection is None:
            return

        try:
            if self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(
                f"Error cerrando la conexión con el middleware: {e}"
            ) from e
        finally:
            self._connection = None
            self._channel = None
            self._consumer_tag = None

    def _assert_connected(self):
        if self._connection is None or not self._connection.is_open:
            raise MessageMiddlewareDisconnectedError(
                "La conexión con el middleware no está disponible"
            )

    def _close_quietly(self):
        try:
            self.close()
        except MessageMiddlewareCloseError:
            pass


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self._exchange_name = exchange_name
        self._routing_keys = list(routing_keys)
        self._queue_name = None
        self._connection = None
        self._channel = None
        self._consumer_tag = None

        if not self._routing_keys:
            raise MessageMiddlewareMessageError(
                "Se requiere al menos una routing key para operar sobre un exchange"
            )

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self._channel = self._connection.channel()
            self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=EXCHANGE_TYPE,
                durable=True,
            )
        except (pika.exceptions.AMQPConnectionError, OSError) as e:
            self._close_quietly()
            raise MessageMiddlewareDisconnectedError(
                f"No se pudo establecer la conexión con el middleware en '{host}': {e}"
            ) from e
        except Exception as e:
            self._close_quietly()
            raise MessageMiddlewareMessageError(
                f"Error inicializando el exchange '{exchange_name}' en '{host}': {e}"
            ) from e

    def start_consuming(self, on_message_callback):
        self._assert_connected()

        if self._consumer_tag is not None:
            raise MessageMiddlewareMessageError(
                "Ya hay un consumo activo sobre esta instancia del middleware"
            )

        def _on_delivery(channel, method, properties, body):
            delivery_tag = method.delivery_tag

            def ack():
                channel.basic_ack(delivery_tag=delivery_tag)

            def nack():
                channel.basic_reject(delivery_tag=delivery_tag, requeue=True)

            on_message_callback(body, ack, nack)

        try:
            if self._queue_name is None:
                declaration = self._channel.queue_declare(queue="", exclusive=True)
                self._queue_name = declaration.method.queue
                for routing_key in self._routing_keys:
                    self._channel.queue_bind(
                        exchange=self._exchange_name,
                        queue=self._queue_name,
                        routing_key=routing_key,
                    )

            self._consumer_tag = self._channel.basic_consume(
                queue=self._queue_name, on_message_callback=_on_delivery
            )
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Se perdió la conexión con el middleware mientras se consumía: {e}"
            ) from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(
                f"Error consumiendo mensajes del exchange '{self._exchange_name}': {e}"
            ) from e
        finally:
            self._consumer_tag = None

    def stop_consuming(self):
        if self._consumer_tag is None:
            return

        self._assert_connected()

        try:
            self._connection.add_callback_threadsafe(self._stop_consuming_now)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Se perdió la conexión con el middleware al detener el consumo: {e}"
            ) from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(
                f"Error deteniendo el consumo del exchange '{self._exchange_name}': {e}"
            ) from e

    def _stop_consuming_now(self):
        if self._consumer_tag is not None:
            self._channel.stop_consuming(self._consumer_tag)

    def send(self, message):
        self._assert_connected()

        try:
            for routing_key in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Se perdió la conexión con el middleware al enviar el mensaje: {e}"
            ) from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(
                f"Error enviando un mensaje al exchange '{self._exchange_name}': {e}"
            ) from e

    def close(self):
        if self._connection is None:
            return

        try:
            if self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(
                f"Error cerrando la conexión con el middleware: {e}"
            ) from e
        finally:
            self._connection = None
            self._channel = None
            self._consumer_tag = None

    def _assert_connected(self):
        if self._connection is None or not self._connection.is_open:
            raise MessageMiddlewareDisconnectedError(
                "La conexión con el middleware no está disponible"
            )

    def _close_quietly(self):
        try:
            self.close()
        except MessageMiddlewareCloseError:
            pass