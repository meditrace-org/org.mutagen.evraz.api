import json
from threading import Thread
import logging

import pika
import requests


class Notifier:
    def __init__(self, webhook_url, mq_host, review_results_queue):
        self._webhook_url = webhook_url
        self._mq_host = mq_host
        self._review_results_queue = review_results_queue

    @property
    def webhook_url(self):
        return self._webhook_url

    @property
    def mq_host(self):
        return self._mq_host

    @property
    def review_results_queue(self):
        return self._review_results_queue

    def send_webhook_notification(self, request_id, file_url):
        """Отправить уведомление через webhook, содержащий ID запроса и URL файла."""
        data = {
            "request_id": request_id,
            "file_url": file_url
        }
        try:
            response = requests.post(self.webhook_url, json=data)
            response.raise_for_status()
            logging.info("Webhook sent successfully!")
        except requests.RequestException as e:
            logging.error(f"Failed to send webhook. Error: {e}")

    def store_result_in_db(self, request_id, file_url):
        """Сохранить результат в базе данных."""
        # TODO
        logging.info(f"Storing result in DB: request_id={request_id}, file_url={file_url}")

    def callback(self, ch, method, properties, body):
        """Функция обратного вызова для обработки сообщений из очереди."""
        try:
            message = json.loads(body)
            request_id = message.get('request_id')
            file_url = message.get('file_url')

            if request_id and file_url:
                self.store_result_in_db(request_id, file_url)
                self.send_webhook_notification(request_id, file_url)
            else:
                logging.warning("Invalid message received: missing request_id or file_url")

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