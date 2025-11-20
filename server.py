import os
import gc
import threading
import asyncio
from fastapi import FastAPI
from embedding_queue import embedding_queue, STOP_SIGNAL
from gpu_worker import gpu_worker
from utils.pdf_processing import process_pdf
from utils.mongo_utils import vector_collection
from utils.s3_utils import list_s3_pdfs, fetch_pdf  # async wrappers

app = FastAPI()

gpu_thread = threading.Thread(target=gpu_worker, daemon=True)

@app.on_event("startup")
def start_gpu_thread():
    print("🚀 Starting embedding worker thread...")
    gpu_thread.start()

@app.on_event("shutdown")
def stop_gpu_thread():
    print("🛑 Stopping embedding worker thread...")
    embedding_queue.put(STOP_SIGNAL)
    gpu_thread.join()

async def process_single_tender(tender_id: str):
    report = {
        "tender_id": tender_id,
        "processed_docs": 0,
        "skipped_docs": 0,
        "empty_docs": 0,
        "scanned_pages": 0,
        "regular_pages": 0,
        "errors": []
    }

    print(f"[{tender_id}] Starting tender processing...")

    try:
        s3_prefix = f"tender-documents/{tender_id}/"
        pdf_keys = await list_s3_pdfs(s3_prefix)
        print(f"[{tender_id}] Found {len(pdf_keys)} PDF(s) in S3.")

        for pdf_key in pdf_keys:
            document_name = os.path.basename(pdf_key)

            # Check if already processed
            doc_exists = await asyncio.to_thread(
                vector_collection.count_documents,
                {"tender_id": tender_id, "document_name": document_name}
            )
            if doc_exists > 0:
                print(f"[{tender_id}] Skipping already processed document: {document_name}")
                report["skipped_docs"] += 1
                continue

            try:
                print(f"[{tender_id}] Fetching PDF: {document_name}")
                pdf_stream = await fetch_pdf(pdf_key)

                print(f"[{tender_id}] Processing PDF: {document_name}")
                pdf_result = await process_pdf(pdf_stream)

                chunks = pdf_result["chunks"]
                report["scanned_pages"] += pdf_result["scanned_pages"]
                report["regular_pages"] += pdf_result["regular_pages"]

                if not chunks:
                    print(f"[{tender_id}] No chunks found in {document_name}")
                    report["empty_docs"] += 1
                    continue

                print(f"[{tender_id}] Queueing {len(chunks)} chunks for embedding: {document_name}")
                embedding_queue.put((chunks, document_name, tender_id))
                report["processed_docs"] += 1

                del pdf_stream, chunks
                gc.collect()
                print(f"[{tender_id}] PDF processed successfully: {document_name}")

            except Exception as e:
                print(f"[{tender_id}] Error processing {document_name}: {e}")
                report["errors"].append(f"{document_name}: {str(e)}")

    except Exception as e:
        print(f"[{tender_id}] Unexpected error: {e}")
        report["errors"].append(str(e))

    print(f"[{tender_id}] Tender processing complete.")
    return report

@app.post("/process/{tender_id}")
async def route_process(tender_id: str):
    tender_id = str(tender_id)
    report = await process_single_tender(tender_id)
    return report


# import os
# import gc
# import threading
# import asyncio
# from fastapi import FastAPI, HTTPException
# from embedding_queue import embedding_queue, STOP_SIGNAL
# from gpu_worker import gpu_worker
# from utils.pdf_processing import process_pdf
# from utils.mongo_utils import vector_collection
# from utils.s3_utils import list_s3_pdfs, fetch_pdf  # async wrappers
# from queue_manager import init_global, get_global  # Your QueueManager code

# app = FastAPI()

# # ---------- GPU embedding worker ----------
# gpu_thread = threading.Thread(target=gpu_worker, daemon=True)

# @app.on_event("startup")
# def start_gpu_thread():
#     print("🚀 Starting embedding worker thread...")
#     gpu_thread.start()

# @app.on_event("shutdown")
# def stop_gpu_thread():
#     print("🛑 Stopping embedding worker thread...")
#     embedding_queue.put(STOP_SIGNAL)
#     gpu_thread.join()


# # ---------- QueueManager ----------
# # Limit to 2 concurrent workers
# queue_manager = init_global(num_workers=2)


# # ---------- Task execution function ----------
# def run_tender_sync(tender_id: str):
#     """
#     This will run in a separate process via QueueManager.
#     Wraps your async `process_single_tender` into sync for multiprocessing.
#     """
#     import asyncio
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     from server import process_single_tender  # import actual function

#     return loop.run_until_complete(process_single_tender(tender_id))


# # ---------- PDF processing logic ----------
# async def process_single_tender(tender_id: str):
#     report = {
#         "tender_id": tender_id,
#         "processed_docs": 0,
#         "skipped_docs": 0,
#         "empty_docs": 0,
#         "scanned_pages": 0,
#         "regular_pages": 0,
#         "errors": []
#     }

#     print(f"[{tender_id}] Starting tender processing...")

#     try:
#         s3_prefix = f"tender-documents/{tender_id}/"
#         pdf_keys = await list_s3_pdfs(s3_prefix)
#         print(f"[{tender_id}] Found {len(pdf_keys)} PDF(s) in S3.")

#         for pdf_key in pdf_keys:
#             document_name = os.path.basename(pdf_key)

#             # Check if already processed
#             doc_exists = await asyncio.to_thread(
#                 vector_collection.count_documents,
#                 {"tender_id": tender_id, "document_name": document_name}
#             )
#             if doc_exists > 0:
#                 print(f"[{tender_id}] Skipping already processed document: {document_name}")
#                 report["skipped_docs"] += 1
#                 continue

#             try:
#                 print(f"[{tender_id}] Fetching PDF: {document_name}")
#                 pdf_stream = await fetch_pdf(pdf_key)

#                 print(f"[{tender_id}] Processing PDF: {document_name}")
#                 pdf_result = await process_pdf(pdf_stream)

#                 chunks = pdf_result["chunks"]
#                 report["scanned_pages"] += pdf_result["scanned_pages"]
#                 report["regular_pages"] += pdf_result["regular_pages"]

#                 if not chunks:
#                     print(f"[{tender_id}] No chunks found in {document_name}")
#                     report["empty_docs"] += 1
#                     continue

#                 print(f"[{tender_id}] Queueing {len(chunks)} chunks for embedding: {document_name}")
#                 embedding_queue.put((chunks, document_name, tender_id))
#                 report["processed_docs"] += 1

#                 del pdf_stream, chunks
#                 gc.collect()
#                 print(f"[{tender_id}] PDF processed successfully: {document_name}")

#             except Exception as e:
#                 print(f"[{tender_id}] Error processing {document_name}: {e}")
#                 report["errors"].append(f"{document_name}: {str(e)}")

#     except Exception as e:
#         print(f"[{tender_id}] Unexpected error: {e}")
#         report["errors"].append(str(e))

#     print(f"[{tender_id}] Tender processing complete.")
#     return report


# # ---------- API endpoints ----------

# @app.post("/process/{tender_id}")
# async def enqueue_tender(tender_id: str):
#     """
#     Instead of processing immediately, enqueue the tender in QueueManager.
#     Returns the current status.
#     """
#     try:
#         qm = get_global()
#         qm.enqueue(tender_id)
#         return {"status": "queued", "tender_id": tender_id}
#     except ValueError as ve:
#         # Already queued or running
#         return {"status": "already_queued_or_running", "tender_id": tender_id}


# @app.get("/status/{tender_id}")
# async def tender_status(tender_id: str):
#     """
#     Get current status of a tender from the queue manager
#     """
#     qm = get_global()
#     status = qm.get_status(tender_id)
#     if not status:
#         raise HTTPException(status_code=404, detail="Tender not found in queue")
#     return status
