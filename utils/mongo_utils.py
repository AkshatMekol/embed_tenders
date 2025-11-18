import gc
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from .config import MONGO_URI, DB_NAME, VECTOR_COLLECTION, TENDERS_COLLECTION, EMBEDDING_MODEL_NAME, device, BATCH_SIZE

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]
vector_collection = db[VECTOR_COLLECTION]
tenders_collection = db[TENDERS_COLLECTION]

model = SentenceTransformer(EMBEDDING_MODEL_NAME, device=device)

def get_tender_ids(min_value: int):
    query = {
        "tender_value": {
            "$gte": min_value,
            "$lte": 100_000_000_000_000  
        }
    }
    projection = {"_id": 1}

    cursor = tenders_collection.find(query, projection)
    return [str(doc["_id"]) for doc in cursor]

def pdf_exists(tender_id: str, document_name: str) -> bool:
    return vector_collection.count_documents({
        "tender_id": tender_id,
        "document_name": document_name
    }) > 0
    
def embed_and_upload_chunks(chunks, document_name, tender_id):
    if not chunks:
        return

    for i in range(0, len(chunks), BATCH_SIZE):
        batch = chunks[i:i + BATCH_SIZE]
        texts = [c["data"] for c in batch]
        embeddings = model.encode(texts, batch_size=BATCH_SIZE, show_progress_bar=False).tolist()

        docs = []
        for c, emb in zip(batch, embeddings):
            docs.append({
                "tender_id": tender_id,
                "document_name": document_name,
                "page": c["page"],
                "position": c["position"],
                "sub_position": c["sub_position"],
                "type": c["type"],
                "is_scanned": c["is_scanned"],
                "text": c["data"],
                "embedding": emb
            })

        vector_collection.insert_many(docs)

    gc.collect()
