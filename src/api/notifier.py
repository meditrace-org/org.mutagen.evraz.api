import asyncio
import json
from threading import Thread
import logging

import pika
import requests


class Notifier:
    def __init__(self, webhook_url, mq_host, review_results_queue, mongo_client):
        self._webhook_url = webhook_url
        self._mq_host = mq_host
        self._review_results_queue = review_results_queue
        self._mongo_client = mongo_client

    @property
    def webhook_url(self):
        return self._webhook_url

    @property
    def mq_host(self):
        return self._mq_host

    @property
    def review_results_queue(self):
        return self._review_results_queue
    
    @property
    def mongo_client(self):
        return self._mongo_client

    def send_webhook_notification(self, data):
        try:
            response = requests.post(self.webhook_url, json=data)
            response.raise_for_status()
            logging.info("Webhook sent successfully!")
        except requests.RequestException as e:
            logging.error(f"Failed to send webhook. Error: {e}")


    def store_result_in_db(self, request_id, data):
        asyncio.run(
            self.mongo_client.insert_or_update(request_id=request_id, data=data)
        )
        logging.info(f"Storing result in DB: data={data}")


    def callback(self, ch, method, properties, body):
        """Функция обратного вызова для обработки сообщений из очереди."""
        try:
            message = json.loads(body)
            request_id = message.get("request_id")
            data = {
                "request_id": request_id,
                "status": message.get("status"),
                "report_file_url": message.get("report_file_url")
            }

            self.store_result_in_db(request_id, data)
            self.send_webhook_notification(data)

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError:
            logging.error("Failed to decode message")

    def listen_to_review_results_queue(self):
        """Слушать очередь 'review_results' для получения сообщений."""
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.mq_host))
        channel = connection.channel()

        channel.queue_declare(queue=self.review_results_queue, durable=True)

        channel.basic_consume(queue=self.review_results_queue, on_message_callback=self.callback)
        logging.info("Listening for messages on the 'review_results' queue...")
        channel.start_consuming()

    def run(self):
        """Запустить новый поток для прослушивания очереди результатов проверки."""
        thread = Thread(target=self.listen_to_review_results_queue)
        thread.daemon = True
        thread.start()