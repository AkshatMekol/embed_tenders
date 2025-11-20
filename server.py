import os
import gc
import threading
import queue
from fastapi import FastAPI
from utils.s3_utils import list_s3_pdfs, fetch_pdf
from utils.pdf_processing import process_pdf
from utils.mongo_utils import vector_collection, store_embeddings_in_db
from utils.embedding_utils import embed_batch

embedding_queue = queue.Queue(maxsize=20000)
STOP_SIGNAL = object()

def gpu_worker():
    print("🚀 GPU worker started...")
    while True:
        task = embedding_queue.get()
        if task is STOP_SIGNAL:
            print("🛑 GPU worker stopping...")
            break

        chunks, document_name, tender_id = task
        try:
            # Batch embed internally
            embeddings = embed_batch(chunks)
            store_embeddings_in_db(embeddings, document_name, tender_id)

        except Exception as e:
            print(f"[GPU Worker] Error: {e}")

        embedding_queue.task_done()


gpu_thread = threading.Thread(target=gpu_worker, daemon=True)

app = FastAPI()

@app.on_event("startup")
def start_gpu_worker_on_startup():
    gpu_thread.start()

@app.on_event("shutdown")
def shutdown_gpu_worker():
    embedding_queue.put(STOP_SIGNAL)
    gpu_thread.join()

def process_single_tender(tender_id):
    report = {
        "tender_id": tender_id,
        "processed_docs": 0,
        "skipped_docs": 0,
        "empty_docs": 0,
        "scanned_pages": 0,
        "regular_pages": 0,
        "errors": []
    }

    print(f"[{tender_id}] Tender started")

    try:
        s3_prefix = f"tender-documents/{tender_id}/"
        pdf_keys = list_s3_pdfs(s3_prefix)

        print(f"[{tender_id}] Found {len(pdf_keys)} PDFs")

        for pdf_key in pdf_keys:
            document_name = os.path.basename(pdf_key)

            # Skip if already processed
            if vector_collection.count_documents({"tender_id": tender_id, "document_name": document_name}) > 0:
                report["skipped_docs"] += 1
                continue

            try:
                pdf_stream = fetch_pdf(pdf_key)
                pdf_result = process_pdf(pdf_stream)

                chunks = pdf_result["chunks"]
                report["scanned_pages"] += pdf_result["scanned_pages"]
                report["regular_pages"] += pdf_result["regular_pages"]

                if not chunks:
                    report["empty_docs"] += 1
                    continue

                embedding_queue.put((chunks, document_name, tender_id))

                report["processed_docs"] += 1

                del pdf_stream, chunks
                gc.collect()

            except Exception as e_pdf:
                report["errors"].append(f"{document_name}: {e_pdf}")

    except Exception as e:
        report["errors"].append(str(e))

    print(f"[{tender_id}] Tender completed")
    return report

@app.post("/process/{tender_id}")
def process_route(tender_id: str):
    return process_single_tender(tender_id)
