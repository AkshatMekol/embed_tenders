from pymongo import MongoClient
from .config import MONGO_URI, DB_NAME, TENDERS_COLLECTION, VECTOR_COLLECTION

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
tenders_collection = db[TENDERS_COLLECTION]
vector_collection = db[VECTOR_COLLECTION]

def get_tender_ids(min_value):
    """Return a list of tender IDs with tender_value >= min_value."""
    cursor = tenders_collection.find(
        {"tender_value": {"$gte": min_value}},
        {"_id": 1}
    )
    return [str(doc["_id"]) for doc in cursor]

def insert_vectors(docs):
    """Insert a list of documents into vector collection."""
    if docs:
        vector_collection.insert_many(docs)

def document_exists_for_tender(tender_id: str, document_name: str) -> bool:
    """Check if a document already exists for a tender."""
    existing = vector_collection.find_one({
        "tender_id": tender_id,
        "document_name": document_name
    })
    return existing is not None
