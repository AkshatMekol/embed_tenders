import os
import gc
from queue import Queue
from threading import Thread
from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from .config import MONGO_URI, DB_NAME, VECTOR_COLLECTION, TENDERS_COLLECTION, EMBEDDING_MODEL_NAME, device, BATCH_SIZE

os.environ["TOKENIZERS_PARALLELISM"] = "false"

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]
vector_collection = db[VECTOR_COLLECTION]
tenders_collection = db[TENDERS_COLLECTION]

print(f"Loading embedding model '{EMBEDDING_MODEL_NAME}' on device: {device}")
model = SentenceTransformer(EMBEDDING_MODEL_NAME, device=device)

embedding_queue = Queue(maxsize=100)

def embedding_worker():
    print("Embedding worker started")
    while True:
        item = embedding_queue.get()
        if item is None:
            print("Embedding worker received shutdown signal")
            embedding_queue.task_done()
            break

        chunks, document_name, tender_id = item
        if not chunks:
            embedding_queue.task_done()
            continue

        print(f"Embedding {len(chunks)} chunks for document '{document_name}' of tender {tender_id}")
        try:
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

                try:
                    vector_collection.insert_many(docs)
                except Exception as e:
                    print(f"❌ Mongo insert error for '{document_name}': {e}")

            print(f"Finished embedding document '{document_name}'")
        except Exception as e:
            print(f"❌ Error embedding document '{document_name}': {e}")

        gc.collect()
        embedding_queue.task_done()

    print("Embedding worker stopped")

embedding_thread = Thread(target=embedding_worker, daemon=True)
embedding_thread.start()
print("Embedding thread initialized and running")

def get_tender_ids(min_value: int):
    query = {"tender_value": {"$gte": min_value, "$lte": 100_000_000_000_000}}
    projection = {"_id": 1}
    cursor = tenders_collection.find(query, projection)
    return [str(doc["_id"]) for doc in cursor]

def pdf_exists(tender_id: str, document_name: str) -> bool:
    return vector_collection.count_documents({
        "tender_id": tender_id,
        "document_name": document_name
    }) > 0

def enqueue_chunks_for_embedding(chunks, document_name, tender_id):
    print(f"📥Queueing {len(chunks)} chunks for embedding: '{document_name}' of tender {tender_id}")
    embedding_queue.put((chunks, document_name, tender_id))
