import asyncio
import json
from datetime import datetime

import aiohttp
import httpx
import markdown2
from fastapi import FastAPI, Request
from uuid import uuid4
from typing import Optional
from contextlib import asynccontextmanager

from starlette.responses import HTMLResponse
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from rabbitmq_client import RabbitMQClient
from config import app_config
from mongodb_client import MongoDBClient
from pydantic import BaseModel, Field, field_validator, HttpUrl
import logging
import utils
from aio_pika.abc import AbstractIncomingMessage

db_client = MongoDBClient(
    database_uri=app_config.mongodb.uri,
    database_name=app_config.mongodb.database,
    collection_name=app_config.common.review_results_coll_name,
    records_ttl=app_config.mongodb.records_ttl
)
mq_client = RabbitMQClient(
    review_results_queue=app_config.rabbitmq.review_results_queue,
    uploaded_to_review_queue=app_config.rabbitmq.uploaded_to_review_queue,
    mq_host=app_config.rabbitmq.mq_host,
    mq_port=app_config.rabbitmq.mq_port,
    mq_username=app_config.rabbitmq.mq_username,
    mq_password=app_config.rabbitmq.mq_password,
    timeout=app_config.rabbitmq.mq_timeout,
    prefetch_count=app_config.rabbitmq.prefetch_count
)
templates = Jinja2Templates(directory=f'{app_config.common.proj_dir}/templates')


class FileUploadRequest(BaseModel):
    target_file_url: HttpUrl = Field(
        description="Ссылка на файл или архив проекта",
        examples=["http://mutagen.org/files/projects/project.zip"]
    )
    instructions_file_url: Optional[HttpUrl] = Field(
        default=None,
        description="Ссылка на файл с инструкциями (PDF)",
        examples=["http://mutagen.org/files/projects/instructions.pdf"]
    )
    last_modified_dttm: Optional[str] = Field(
        default=None,
        description="Время последнего изменения в формате ISO.",
        examples=["2024-10-10T12:30:00+03:00"]
    )

    @field_validator("last_modified_dttm", mode="before")
    def validate_last_modified_dttm(cls, value):
        if value is None:
            return value
        try:
            datetime.fromisoformat(value)
        except ValueError:
            raise ValueError("last_modified_dttm должен быть в формате ISO (пример 2024-10-10T12:30:00+03:00)")
        return value

    @field_validator("target_file_url", "instructions_file_url", mode="before")
    def validate_url_content_type(cls, value, info):
        if value is None:
            return value

        try:
            with httpx.Client(timeout=5.0) as client:
                response = client.head(value, follow_redirects=True)

            if response.status_code != 200:
                raise ValueError(f"URL {value} недоступен: статус {response.status_code}")

            content_type = response.headers.get("Content-Type", "")
            target_file_content_types = [
                "application/zip",
                "application/x-rar-compressed",
                "application/x-tar",
                "application/octet-stream"
            ]
            instructions_file_content_types = [
                "application/pdf",
                "application/octet-stream"
            ]
            if info.field_name == "target_file_url":
                if not any(ct in content_type for ct in target_file_content_types):
                    raise ValueError(f"URL {value} должен указывать на архив (Content-Type: {content_type})")
            elif info.field_name == "instructions_file_url":
                if not any(ct in content_type for ct in instructions_file_content_types):
                    raise ValueError(f"URL {value} должен указывать на PDF файл (Content-Type: {content_type})")

        except httpx.RequestError as e:
            raise ValueError(f"URL {value} недоступен: {e}")

        return value


class FileUploadResponse(BaseModel):
    request_id: str = Field(description="Уникальный идентификатор запроса")


class StatusResponse(BaseModel):
    request_id: str = Field(description="Уникальный идентификатор запроса")
    status: str = Field(description="Статус запроса")
    report_content: Optional[str] = Field(description="Текст отчета в формате markdown", default=None)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_client, mq_client
    db_client.connect()
    connect_task = asyncio.create_task(mq_client.connect())

    try:
        yield
    finally:
        if mq_client:
            connect_task.cancel()
            await mq_client.close()


app = FastAPI(lifespan=lifespan)


@app.post(
    "/upload",
    summary="Загрузка файлов проекта для его дальнейшего ревью",
    response_model=FileUploadResponse,
    status_code=202
)
async def handle_upload_file(request: FileUploadRequest):
    if db_client is None:
        logging.error("db_client is not initialized")
        raise ValueError("db_client is not initialized")

    request_id = str(uuid4())
    data = {
        "request_id": request_id,
        "target_file_url": str(request.target_file_url),
        "instructions_file_url": str(request.instructions_file_url) if request.instructions_file_url else None,
        "last_modified_dttm": request.last_modified_dttm,
        "status": "received"
    }

    await db_client.insert_or_update(request_id=request_id, data=data)
    await mq_client.publish(data=data)

    return FileUploadResponse(request_id=request_id)


@app.get(
    "/status/{request_id}",
    summary="Проверка статуса запроса",
    response_model=StatusResponse,
    status_code=200
)
async def get_status(request_id: str):
    if db_client is None:
        logging.error("db_client is not initialized")
        raise ValueError("db_client is not initialized")

    result = await db_client.get_by_request_id(request_id=request_id)
    response = utils.build_report_response(result)
    return StatusResponse(**response)


@mq_client.on_message
async def on_message(message: AbstractIncomingMessage):
    async with message.process():
        body = message.body
        data = utils.build_report_response(json.loads(body))

        # save result in mongo
        await db_client.insert_or_update(request_id=data.get("request_id"), data=data)

        # send to rabbitmq
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(app_config.common.webhook_url, json=data) as response:
                    response.raise_for_status()
                    logging.info(f'Webhook sent successfully: {data}')
        except aiohttp.ClientError as e:
            logging.error(f'Failed to send webhook. Error: {e}')


@app.get(
    "/reports/{request_id}",
    summary="Просмотреть отчет",
    response_class=HTMLResponse,
    status_code=200,
    responses={
        404: {"description": "Отчет не найден"},
        425: {"description": "Отчет не готов"}
    }
)
async def get_report(request: Request, request_id: str):
    data = await db_client.get_by_request_id(request_id=request_id)
    if data is None:
        return HTMLResponse(status_code=404)
    md_content = data.get("report_content")
    if md_content is None:
        return HTMLResponse(status_code=425)
    html_content = markdown2.markdown(md_content, extras=[
        "fenced-code-blocks", "tables", "header-ids", "github",
        "syntax-highlighting", "autolinks", "footnotes"
    ])
    return templates.TemplateResponse(
        request=request,
        name="report_template.html",
        context={"html_content": html_content}
    )


if __name__ == "__main__":
    import uvicorn

    app.mount(
        '/static',
        StaticFiles(directory=f'{app_config.common.proj_dir}/static'),
        name='static'
    )
    uvicorn.run(app, host="0.0.0.0", port=app_config.common.port, log_config=None)
