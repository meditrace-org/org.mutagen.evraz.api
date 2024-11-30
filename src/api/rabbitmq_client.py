import asyncio
import json
import logging

import aio_pika
from aio_pika import connect
from aio_pika.abc import TimeoutType

class RabbitMQClient:
    def __init__(self, review_results_queue: str, uploaded_to_review_queue: str,
                 mq_host: str, mq_port: int, mq_username: str, mq_password: str,
                 prefetch_count: int, timeout: TimeoutType = None, reconnect_interval: int = 5):
        self._should_reconnect = True
        self._consume_task = None
        self.connection = None
        self.reconnect_interval = reconnect_interval
        self._review_results_queue = review_results_queue
        self._uploaded_to_review_queue = uploaded_to_review_queue
        self._mq_host = mq_host
        self._mq_port = mq_port
        self._mq_username = mq_username
        self._mq_password = mq_password
        self._prefetch_count = prefetch_count
        self.timeout = timeout
        self.channel = None
        self._message_handler = None


    @property
    def review_results_queue(self):
        return self._review_results_queue


    @property
    def uploaded_to_review_queue(self):
        return self._uploaded_to_review_queue


    def on_message(self, func):
        self._message_handler = func
        return func


    async def connect(self) -> None:
        while True:
            try:
                self.connection = await connect(
                    host=self._mq_host,
                    port=self._mq_port,
                    login=self._mq_username,
                    password=self._mq_password,
                    timeout=self.timeout
                )
                logging.info("Connected to RabbitMQ.")
                break
            except Exception as e:
                logging.error(f"Error connecting to RabbitMQ: {e}")
                if not self._should_reconnect:
                    break
                logging.info(f"Retrying in {self.reconnect_interval} seconds...")
                await asyncio.sleep(self.reconnect_interval)

        async with self.connection:
            self.channel = await self.connection.channel()
            self.channel.close_callbacks.add(self._on_close_channel)
            await self.channel.set_qos(prefetch_count=self._prefetch_count)

            queue = await self.channel.declare_queue(
                name=self.review_results_queue,
                durable=False,
                auto_delete=False,
                arguments={
                    'x-message-ttl': 3600000
                }
            )
            if self._message_handler:
                await queue.consume(self._message_handler, no_ack=False)
            self._consume_task = asyncio.Future()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                logging.info("Consume task was cancelled due to connection closure.")


    async def _on_close_channel(self, *args):
        if not self._consume_task.done():
            self._consume_task.cancel()
        if self._should_reconnect:
            logging.info(f"Channel closed, reconnecting in {self.reconnect_interval} seconds...")
            await self.connect()


    async def close(self, should_reconnect: bool = False) -> None:
        self._should_reconnect = should_reconnect
        if self.channel and not self.channel.is_closed:
            try:
                await self.channel.close()
                logging.info("RabbitMQ connection closed successfully.")
            except Exception as e:
                logging.error(f"Error closing RabbitMQ connection: {e}")


    async def publish(self, data: dict) -> None:
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(data).encode()),
            routing_key=self.uploaded_to_review_queue
        )
