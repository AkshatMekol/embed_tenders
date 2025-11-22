import gc
import torch
from embedding_queue import embedding_queue, STOP_SIGNAL
from utils.embedding_utils import embed_batch
from utils.mongo_utils import store_embeddings_in_db, vector_collection

def gpu_worker():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"🚀 GPU worker running on: {device}")

    while True:
        task = embedding_queue.get()

        if task is STOP_SIGNAL:
            print("🛑 GPU worker stopping (STOP signal)")
            break

        chunks, document_name, tender_id, is_last_batch = task

        for c in chunks:
            c["tender_id"] = tender_id
            c["document_name"] = document_name

        try:
            embeddings = embed_batch(chunks)
            print(f"[{document_name}] 🔹 Vectorized {len(chunks)} chunks")

            store_embeddings_in_db(embeddings, document_name, tender_id)
            print(f"[{document_name}] 💾 Stored in MongoDB")

            # If this was the last batch, mark complete
            if is_last_batch:
                vector_collection.update_one(
                    {"tender_id": tender_id, "document_name": document_name},
                    {"$set": {"document_complete": True}},
                    upsert=True
                )
                print(f"[{document_name}] 🎉 Document marked COMPLETE")

        except Exception as e:
            print(f"[GPU WORKER] ❌ Error: {document_name}: {e}")

        gc.collect()
        embedding_queue.task_done()

    print("🛑 GPU worker exited")

