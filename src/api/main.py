from fastapi import FastAPI
from uuid import uuid4
from typing import Optional
from datetime import datetime
from rabbitmq_client import RabbitMQClient
from config import app_config
from mongodb_client import MongoDBClient
from pydantic import BaseModel, Field

app = FastAPI()
db_client = None


class FileUploadRequest(BaseModel):
    target_file_url: str = Field(
        description="Ссылка на архив проекта",
        examples = ["http://mutagen.org/files/projects/project.zip"]
    )
    instructions_file_url: Optional[str] = Field(
        default=None,
        description="Ссылка на файл с инструкциями к проекту",
        examples=["http://mutagen.org/files/projects/instructions.zip"]
    )
    last_modified_dttm: Optional[str] = Field(
        default=None,
        description="Время последнего изменения в формате ISO.",
        examples=["2024-10-10T12:30:00+03:00"]
    )


def convert_to_unix_timestamp(iso_date_str: Optional[str]) -> Optional[int]:
    if iso_date_str:
        return int(datetime.fromisoformat(iso_date_str.rstrip('Z')).timestamp())
    return None


@app.post(
    "/upload",
    summary="Загрузка файлов проекта для его дальнейшего ревью"
)
class FileUploadResponse(BaseModel):
    request_id: str = Field(description="Уникальный идентификатор запроса")

class StatusResponse(BaseModel):
    request_id: str = Field(description="Уникальный идентификатор запроса")
    status: str = Field(description="Статус запроса")
    report_file_url: Optional[str] = Field(description="Ссылка на файл с отчётом", default=None)

@app.post(
    "/upload",
    summary="Загрузка файлов проекта для его дальнейшего ревью",
    response_model=FileUploadResponse,
    status_code=202
)
async def handle_upload_file(request: FileUploadRequest):
    request_id = str(uuid4())
    last_modified_unix = convert_to_unix_timestamp(request.last_modified_dttm)
    data = {
        "request_id": request_id,
        "target_file_url": request.target_file_url,
        "instructions_file_url": request.instructions_file_url,
        "last_modified_dttm": last_modified_unix,
        "status": "received"
    }
    await db_client.insert_or_update(request_id=request_id, data=data)
    await mq_client.publish(data=data)
    return FileUploadResponse(request_id=request_id), 202


@app.get(
    "/status/{request_id}",
    summary="Проверка статуса запроса",
    response_model=StatusResponse,
    status_code=200
)
async def get_status(request_id: str):
    result = await db_client.get_by_request_id(request_id=request_id)
    response = {
        "request_id": result.get("request_id"),
        "status": result.get("status"),
        "report_file_url": result.get("report_file_url")
    }
    return StatusResponse(**response)


if __name__ == "__main__":
    db_client = MongoDBClient(
        database_uri=app_config.mongodb.uri,
        database_name=app_config.mongodb.database,
        collection_name=app_config.common.review_results_coll_name,
        records_ttl=app_config.mongodb.records_ttl
    )
    mq_client = RabbitMQClient(
        webhook_url=app_config.common.webhook_url,
        mongo_client=db_client,
        review_results_queue=app_config.rabbitmq.review_results_queue,
        uploaded_to_review_queue=app_config.rabbitmq.uploaded_to_review_queue,
        mq_host=app_config.rabbitmq.mq_host,
        mq_port=app_config.rabbitmq.mq_port,
        mq_username=app_config.rabbitmq.mq_username,
        mq_password=app_config.rabbitmq.mq_password,
        timeout=app_config.rabbitmq.mq_timeout,
        prefetch_count=app_config.rabbitmq.prefetch_count
    )
    mq_client.connect()
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)

