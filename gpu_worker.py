import gc
from embedding_queue import embedding_queue, STOP_SIGNAL
from utils.embedding_utils import embed_batch
from utils.mongo_utils import store_embeddings_in_db

def gpu_worker():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"🚀 GPU worker started on device: {device}")

    while True:
        task = embedding_queue.get()

        if task is STOP_SIGNAL:
            print("🛑 GPU worker received stop signal. Exiting.")
            break

        chunks, document_name, tender_id = task

        try:
            embeddings = embed_batch(chunks)
            print(f"[{document_name}] ✅ Successfully vectorized {len(chunks)} chunks")

            store_embeddings_in_db(embeddings, document_name, tender_id)
            print(f"[{document_name}] 💾 Successfully stored embeddings in MongoDB")

        except Exception as e:
            print(f"[GPU WORKER] ❌ Error embedding {document_name}: {e}")

        gc.collect()
        embedding_queue.task_done()

    print("🛑 GPU worker stopped.")
