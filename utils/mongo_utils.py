from pymongo import MongoClient
from utils.config import MONGO_URI, DB_NAME, VECTOR_COLLECTION, TENDERS_COLLECTION

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]

vector_collection = db[VECTOR_COLLECTION]
tenders_collection = db[TENDERS_COLLECTION]

def store_embeddings_in_db(embeddings, document_name, tender_id):
    try:
        vector_collection.insert_many(embeddings)
    except Exception as e:
        print(f"❌ Mongo Insert Error: {e}")
