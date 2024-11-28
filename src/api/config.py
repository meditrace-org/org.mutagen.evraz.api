import logging
import os
from pydantic import BaseModel, BaseSettings, PositiveInt


def configure_logging():
    logging.basicConfig(level=logging.INFO)


class RabbitMQConfig(BaseModel):
    review_results_queue: str = os.getenv("REVIEW_RESULTS_QUEUE")
    mq_host: str = os.getenv("MQ_HOST")
    mq_port: PositiveInt = int(os.getenv("MQ_PORT"))


class MongoDBConfig(BaseModel):
    _username: str = os.getenv("MONGO_INITDB_ROOT_USERNAME")
    _password: str = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
    _host: str = os.getenv("MONGO_HOST")
    _port: PositiveInt = int(os.getenv("MONGO_PORT"))
    database: str = os.getenv("MONGO_INITDB_DATABASE", "default_database")
    records_ttl: PositiveInt = int(os.getenv("MONGO_RECORDS_TTL"))

    @property
    def uri(self) -> str:
        return f"mongodb://{self._username}:{self._password}@{self._host}:{self._port}/{self.database}"


class CommonConfig(BaseModel):
    webhook_url: str = os.getenv("REVIEW_RESULTS_WEBHOOK_URL")
    review_results_coll_name = os.getenv("REVIEW_RESULTS_COLLECTION_NAME")


class AppConfig(BaseSettings):
    common: CommonConfig = CommonConfig()
    rabbitmq: RabbitMQConfig = RabbitMQConfig()
    mongodb: MongoDBConfig = MongoDBConfig()

    class Config:
        env_nested_delimiter = '__'


configure_logging()
app_config = AppConfig()