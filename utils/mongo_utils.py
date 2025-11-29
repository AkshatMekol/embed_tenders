from bson.objectid import ObjectId
from pymongo import MongoClient, ReturnDocument
from .config import MONGO_URI, DB_NAME, VECTOR_COLLECTION, TENDERS_COLLECTION, DOCS_STATUS_COLLECTION

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]

vector_collection = db[VECTOR_COLLECTION]
tenders_collection = db[TENDERS_COLLECTION]
docs_status_collection = db[DOCS_STATUS_COLLECTION]
ALLOWED_INDUSTRIES = ["Water & Sanitation", "Power & Energy"]

def store_embeddings_in_db(embeddings, document_name, tender_id):
    try:
        vector_collection.insert_many(embeddings)
    except Exception as e:
        print(f"❌ Mongo Insert Error: {e}")

def get_tender_ids(min_value):
    cursor = tenders_collection.find(
        {
            "tender_value": {"$gte": min_value},
            "industries": {"$in": ALLOWED_INDUSTRIES}  
        },
        {"_id": 1}
    )
    return [str(doc["_id"]) for doc in cursor]

def is_document_complete(tender_id, document_name):
    record = docs_status_collection.find_one(
        {"tender_id": tender_id, "completed_documents": document_name}
    )
    return record is not None

def mark_document_complete(tender_id, document_name):
    docs_status_collection.update_one(
        {"tender_id": tender_id},
        {"$addToSet": {"completed_documents": document_name}},
        upsert=True
    )
