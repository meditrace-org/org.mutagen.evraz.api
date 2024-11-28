from fastapi import FastAPI
from uuid import uuid4
from typing import Optional
from notifier import Notifier
from config import app_config
from mongodb_client import MongoDBClient
from pydantic import BaseModel, Field

app = FastAPI()

STATUS_RECEIVED = "received"


def init_db_client():
    return MongoDBClient(
        database_uri=app_config.mongodb.uri,
        database_name=app_config.mongodb.database_name,
        collection_name=app_config.common.review_results_coll_name,
        records_ttl=app_config.mongodb.records_ttl
    )


def init_notifier():
    return Notifier(
        webhook_url=app_config.common.webhook_url,
        mq_host=app_config.rabbitmq.mq_host,
        review_results_queue=app_config.rabbitmq.review_results_queue
    )


db_client = init_db_client()


class FileUploadRequest(BaseModel):
    target_file_url: str
    instructions_file_url: Optional[str] = Field(default=None)
    last_modified_dttm: Optional[str] = Field(default=None)


@app.post("/upload")
async def handle_upload_file(request: FileUploadRequest):
    request_id = str(uuid4())
    data = {
        "target_file_url": request.file_url,
        "instructions_file_url": request.instructions_file_url,
        "last_modified_dttm": request.last_modified_dttm,
        "status": STATUS_RECEIVED
    }
    await db_client.insert_or_update(request_id=request_id, data=data)
    # TODO: отправить в очередь
    return {"request_id": request_id}, 202


@app.get("/status/{request_id}")
async def get_status(request_id: str):
    return db_client.get_by_request_id(request_id=request_id)


if __name__ == "__main__":
    notifier = init_notifier()
    notifier.run()
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
