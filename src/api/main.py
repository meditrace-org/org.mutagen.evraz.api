from fastapi import FastAPI
from uuid import uuid4
from typing import Optional
from datetime import datetime
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
    last_modified_unix = (
        int(datetime.fromisoformat(request.last_modified_dttm.rstrip('Z')).timestamp())
        if request.last_modified_dttm else None
    )
    data = {
        "request_id": request_id,
        "target_file_url": request.target_file_url,
        "instructions_file_url": request.instructions_file_url,
        "last_modified_dttm": last_modified_unix,
        "status": STATUS_RECEIVED
    }
    await db_client.insert_or_update(request_id=request_id, data=data)
    # TODO: отправить в очередь
    return {"request_id": request_id}, 202


@app.get("/status/{request_id}")
async def get_status(request_id: str):
    result = await db_client.get_by_request_id(request_id=request_id)
    response = {
        "request_id": result["request_id"],
        "status": result["status"],
    }
    if "report_file_url" in result:
        response["report_file_url"] = result["report_file_url"]
    return response


if __name__ == "__main__":
    notifier = init_notifier()
    notifier.run()
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
