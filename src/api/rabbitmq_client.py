import asyncio
import json
import logging

import aio_pika
from aio_pika import connect
from aio_pika.abc import AbstractIncomingMessage, TimeoutType
import aiohttp

from mongodb_client import MongoDBClient

class RabbitMQClient:
    def __init__(self, webhook_url: str, mongo_client: MongoDBClient,
                 review_results_queue: str, uploaded_to_review_queue: str,
                 mq_host: str, mq_port: int, mq_username: str, mq_password: str,
                 prefetch_count: int, timeout: TimeoutType = None, reconnect_interval: int = 5):
        self._consume_task = None
        self.connection = None
        self.reconnect_interval = reconnect_interval
        self._webhook_url = webhook_url
        self._mongo_client = mongo_client
        self._review_results_queue = review_results_queue
        self._uploaded_to_review_queue = uploaded_to_review_queue
        self._mq_host = mq_host
        self._mq_port = mq_port
        self._mq_username = mq_username
        self._mq_password = mq_password
        self._prefetch_count = prefetch_count
        self.timeout = timeout
        self.channel = None


    @property
    def review_results_queue(self):
        return self._review_results_queue


    @property
    def uploaded_to_review_queue(self):
        return self._uploaded_to_review_queue


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
                logging.info("Retrying in 5 seconds...")
                await asyncio.sleep(5)

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
            await queue.consume(self._on_message, no_ack=False)
            self._consume_task = asyncio.Future()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                logging.info("Consume task was cancelled due to connection closure.")

    async def _on_close_channel(self, *args):
        if not self._consume_task.done():
            self._consume_task.cancel()
        logging.info(f"Channel closed, reconnecting in {self.reconnect_interval} seconds...")
        await self.connect()

    async def close(self) -> None:
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


    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        async with message.process():
            body = message.body
            message = json.loads(body)
            request_id = message.get('request_id')
            data = {
                'request_id': request_id,
                'status': message.get('status'),
                'report_file_url': message.get('report_file_url')
            }
            await self._store_result_in_db(request_id, data)
            await self._send_webhook_notification(data)


    async def _send_webhook_notification(self, data: dict) -> None:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(self._webhook_url, json=data) as response:
                    response.raise_for_status()
                    logging.info(f'Webhook sent successfully: {data}')
        except aiohttp.ClientError as e:
            logging.error(f'Failed to send webhook. Error: {e}')


    async def _store_result_in_db(self, request_id: str, data: dict) -> None:
        await self._mongo_client.insert_or_update(request_id=request_id, data=data)
        logging.info(f'Storing result in DB: data={data}')
