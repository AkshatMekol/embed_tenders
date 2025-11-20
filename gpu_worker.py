import gc
from .embedding_queue import embedding_queue, STOP_SIGNAL
from utils.embedding_utils import embed_batch
from utils.mongo_utils import store_embeddings_in_db

def gpu_worker():
    print("🚀 GPU worker started.")
    while True:
        task = embedding_queue.get()

        if task is STOP_SIGNAL:
            print("🛑 GPU worker received stop signal.")
            break

        chunks, document_name, tender_id = task

        try:
            embeddings = embed_batch(chunks)
            store_embeddings_in_db(embeddings, document_name, tender_id)
        except Exception as e:
            print(f"[GPU WORKER] ❌ Error embedding {document_name}: {e}")

        gc.collect()
        embedding_queue.task_done()

    print("GPU worker stopped.")
