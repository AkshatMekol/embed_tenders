import motor.motor_asyncio
from .config import MONGO_URI, DB_NAME, TENDERS_COLLECTION, VECTOR_COLLECTION

client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
tenders_collection = db[TENDERS_COLLECTION]
vector_collection = db[VECTOR_COLLECTION]

async def get_tender_ids(min_value):
    cursor = tenders_collection.find({"tender_value": {"$gte": min_value}}, {"_id": 1})
    return [str(doc["_id"]) async for doc in cursor]

async def insert_vectors(docs):
    if docs:
        await vector_collection.insert_many(docs)


async def folder_exists_for_tender(tender_id: str, document_name: str) -> bool:
    existing = await vector_collection.find_one({
        "tender_id": tender_id,
        "document_name": document_name
    })
    return existing is not None
