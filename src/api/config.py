import asyncio
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

from pydantic import BaseModel, PositiveInt
from pydantic_settings import BaseSettings


class CoroutineFormatter(logging.Formatter):
    def format(self, record):
        current_task = asyncio.current_task()
        if current_task:
            record.coroutine_name = current_task.get_name()
        else:
            record.coroutine_name = "MainThread"
        return super().format(record)

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
            return s[:-3]
        else:
            t = dt.strftime('%Y-%m-%d %H:%M:%S')
            return f"{t}.{int(record.msecs):03d}"


log_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data')
if not os.path.exists(log_dir):
    os.makedirs(log_dir)


def configure_logging():
    start_time = datetime.now().strftime("%d_%m_%Y__%H_%M_%S")
    handler = RotatingFileHandler(
        os.path.join(log_dir, f'log_{start_time}.txt'),
        maxBytes=5 * 1024 * 1024,
        backupCount=10
    )

    formatter = CoroutineFormatter(
        '%(asctime)s [%(coroutine_name)s] %(levelname)s %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S.%f'
    )
    handler.setFormatter(formatter)
    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler, logging.StreamHandler()]
    )


class RabbitMQConfig(BaseModel):
    review_results_queue: str = os.getenv("REVIEW_RESULTS_QUEUE")
    uploaded_to_review_queue: str = os.getenv("UPLOADED_TO_REVIEW_QUEUE")
    mq_host: str = os.getenv("MQ_HOST")
    mq_port: PositiveInt = int(os.getenv("MQ_PORT"))
    mq_username: str = os.getenv("MQ_USERNAME")
    mq_password: str = os.getenv("MQ_PASSWORD")
    mq_timeout: PositiveInt = int(os.getenv("MQ_TIMEOUT"))
    prefetch_count: int = int(os.getenv("MQ_PREFETCH_COUNT"))


class MongoDBConfig(BaseModel):
    _username: str = os.getenv("MONGO_INITDB_ROOT_USERNAME")
    _password: str = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
    _host: str = os.getenv("MONGO_HOST")
    _port: PositiveInt = int(os.getenv("MONGO_PORT"))
    database: str = os.getenv("MONGO_INITDB_DATABASE", "default_database")
    records_ttl: PositiveInt = int(os.getenv("MONGO_RECORDS_TTL"))

    @property
    def uri(self) -> str:
        return f"mongodb://{self._username}:{self._password}@{self._host}:{self._port}/{self.database}?authSource=admin"


class CommonConfig(BaseModel):
    webhook_url: str = os.getenv("REVIEW_RESULTS_WEBHOOK_URL")
    review_results_coll_name: str = os.getenv("REVIEW_RESULTS_COLLECTION_NAME")
    port: int = int(os.getenv("EVRAZ_API_PORT"))


class AppConfig(BaseSettings):
    common: CommonConfig = CommonConfig()
    rabbitmq: RabbitMQConfig = RabbitMQConfig()
    mongodb: MongoDBConfig = MongoDBConfig()

    class Config:
        env_nested_delimiter = '__'


configure_logging()
app_config = AppConfig()