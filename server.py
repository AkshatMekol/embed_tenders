import os
import gc
import threading
from fastapi import FastAPI
from utils.s3_utils import list_s3_pdfs, fetch_pdf
from utils.pdf_processing import process_pdf
from utils.mongo_utils import vector_collection
from embedding_queue import embedding_queue, STOP_SIGNAL
from gpu_worker import gpu_worker

app = FastAPI()

gpu_thread = threading.Thread(target=gpu_worker, daemon=True)

@app.on_event("startup")
def start_gpu_thread():
    gpu_thread.start()

@app.on_event("shutdown")
def stop_gpu_thread():
    embedding_queue.put(STOP_SIGNAL)
    gpu_thread.join()

async def process_single_tender(tender_id):
    report = {
        "tender_id": tender_id,
        "processed_docs": 0,
        "skipped_docs": 0,
        "empty_docs": 0,
        "scanned_pages": 0,
        "regular_pages": 0,
        "errors": []
    }

    print(f"[{tender_id}] Starting tender.")

    try:
        s3_prefix = f"tender-documents/{tender_id}/"
        pdf_keys = list_s3_pdfs(s3_prefix)

        for pdf_key in pdf_keys:
            document_name = os.path.basename(pdf_key)

            if vector_collection.count_documents({"tender_id": tender_id, "document_name": document_name}) > 0:
                report["skipped_docs"] += 1
                continue

            try:
                pdf_stream = fetch_pdf(pdf_key)
                pdf_result = await process_pdf(pdf_stream)

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

            except Exception as e:
                report["errors"].append(str(e))

    except Exception as e:
        report["errors"].append(str(e))

    return report

@app.post("/process/{tender_id}")
async def route_process(tender_id: str):
    return async process_single_tender(tender_id)
