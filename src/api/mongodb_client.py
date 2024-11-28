from motor.motor_asyncio import AsyncIOMotorClient
from fastapi import HTTPException
import os
from datetime import datetime, timedelta, timezone


class MongoDBClient:
    def __init__(self, database_uri: str, database_name: str, collection_name: str, records_ttl: int):
        self._records_ttl = records_ttl
        self.client = AsyncIOMotorClient(os.getenv(database_uri))

        self.database = self.client[database_name]
        self.collection = self.database[collection_name]
        self._create_ttl_index()

    def _create_ttl_index(self):
        # Создание TTL индекса на поле `expiration_dttm`
        self.collection.create_index("expiration_dttm", expireAfterSeconds=self._records_ttl)

    async def insert_or_update(self, request_id: str, data: dict):
        data["_id"] = request_id
        data["expiration_dttm"] = datetime.now(timezone.utc) + timedelta(days=7)
        await self.collection.replace_one({"_id": request_id}, data, upsert=True)

    async def get_by_request_id(self, request_id: str) -> dict:
        document = await self.collection.find_one({"_id": request_id})
        if not document:
            raise HTTPException(status_code=404, detail="Request not found")
        return document
