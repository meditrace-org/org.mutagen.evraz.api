import asyncio
import json
import logging

import aio_pika
from aio_pika import connect
from aio_pika.abc import AbstractIncomingMessage, TimeoutType
from aiogram.client.session import aiohttp

from mongodb_client import MongoDBClient

class RabbitMQClient:
    def __init__(self, webhook_url: str, mongo_client: MongoDBClient,
                 review_results_queue: str, uploaded_to_review_queue: str,
                 mq_host: str, mq_port: int, mq_username: str, mq_password: str,
                 prefetch_count: int, timeout: TimeoutType = None):
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
        connection = await connect(
            host=self._mq_host,
            port=self._mq_port,
            login=self._mq_username,
            password=self._mq_password,
            timeout=self.timeout
        )
        async with connection:
            self.channel = await connection.channel()
            await self.channel.set_qos(prefetch_count=self._prefetch_count)
            queue = await self.channel.declare_queue(name=self.review_results_queue)
            await queue.consume(self._on_message, no_ack=False)
            await asyncio.Future()


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
