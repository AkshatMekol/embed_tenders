from fastapi.middleware.cors import CORSMiddleware
import os
import gc
from io import BytesIO
import threading
import asyncio
from fastapi import FastAPI, HTTPException
from embedding_queue import embedding_queue, STOP_SIGNAL
from gpu_worker import gpu_worker
from utils.pdf_processing import process_pdf_batch
from utils.mongo_utils import vector_collection
from utils.s3_utils import list_s3_pdfs, fetch_pdf
import pdfplumber

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
    print("✅ Shutdown complete")

origins = [
    "http://localhost:8080",
    "http://192.168.1.5:8080",
    "https://tenderbharat.vercel.app",
    "http://localhost:3000",
    "https://www.bidindia.site",
    "https://www.bidindia.co.in",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PDF_BATCH_SIZE = 20
tender_semaphore = asyncio.Semaphore(4)
# max 4 concurrent tenders

async def process_single_tender(tender_id: str):
    print(f"[{tender_id}] Starting tender processing...")

    report = {
        "tender_id": tender_id,
        "processed_docs": 0,
        "skipped_docs": 0,
        "empty_docs": 0,
        "scanned_pages": 0,
        "regular_pages": 0,
        "errors": []
    }

    try:
        s3_prefix = f"tender-documents/{tender_id}/"
        pdf_keys = await list_s3_pdfs(s3_prefix)
        print(f"[{tender_id}] Found {len(pdf_keys)} PDFs")

        for pdf_key in pdf_keys:

            document_name = os.path.basename(pdf_key)
            print(f"[{tender_id}] Processing document: {document_name}")

            existing = await asyncio.to_thread(
                vector_collection.find_one,
                {"tender_id": tender_id, "document_name": document_name},
                {"document_complete": 1}
            )

            if existing and existing.get("document_complete"):
                print(f"[{tender_id}] Skipping already processed doc: {document_name}")
                report["skipped_docs"] += 1
                continue

            if existing:
                print(f"[{tender_id}] Removing partial embeddings of {document_name}")
                await asyncio.to_thread(
                    vector_collection.delete_many,
                    {"tender_id": tender_id, "document_name": document_name}
                )

            try:
                pdf_stream = await fetch_pdf(pdf_key)
                pdf_bytes = pdf_stream.read()
                pdf_io = BytesIO(pdf_bytes)

                total_pages = await asyncio.to_thread(
                    lambda: len(pdfplumber.open(pdf_io).pages)
                )

                print(f"[{tender_id}] {document_name}: {total_pages} pages")

                for start in range(0, total_pages, PDF_BATCH_SIZE):
                    end = start + PDF_BATCH_SIZE
                    print(f"[{tender_id}] Processing batch {start}-{end}")

                    chunks, scanned, regular = await process_pdf_batch(
                        pdf_bytes, start, end
                    )

                    report["scanned_pages"] += scanned
                    report["regular_pages"] += regular

                    if chunks:
                        print(f"[{tender_id}] Queueing {len(chunks)} chunks for embedding")
                        embedding_queue.put((chunks, document_name, tender_id))

                    gc.collect()

                await asyncio.to_thread(
                    vector_collection.update_one,
                    {"tender_id": tender_id, "document_name": document_name},
                    {"$set": {"document_complete": True}},
                    True
                )

                report["processed_docs"] += 1
                print(f"[{tender_id}] Completed {document_name}")

            except Exception as e:
                msg = f"{document_name}: {str(e)}"
                print(f"[{tender_id}] ERROR: {msg}")
                report["errors"].append(msg)

    except Exception as e:
        print(f"[{tender_id}] UNEXPECTED ERROR: {e}")
        report["errors"].append(str(e))

    print(f"[{tender_id}] Tender processing complete.")
    return report


@app.post("/process/{tender_id}")
async def route_process(tender_id: str):
    tender_id = str(tender_id)
    print(f"[{tender_id}] Received request")

    async with tender_semaphore:
        try:
            return await process_single_tender(tender_id)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


# from fastapi.middleware.cors import CORSMiddleware
# import os
# import gc
# import threading
# import asyncio
# from fastapi import FastAPI, HTTPException
# from embedding_queue import embedding_queue, STOP_SIGNAL
# from gpu_worker import gpu_worker
# from utils.pdf_processing import process_pdf
# from utils.mongo_utils import vector_collection
# from utils.s3_utils import list_s3_pdfs, fetch_pdf 

# app = FastAPI()

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

# origins = [
#     "http://localhost:8080",
#     "http://192.168.1.5:8080",
#     "https://tenderbharat.vercel.app",
#     "http://localhost:3000",
#     # "https://tender-bharat.com",
#     # "https://www.tender-bharat.com",
#     # "https://www.tender-bharat.site",
#     "https://www.bidindia.site",
#     "https://www.bidindia.co.in",
# ]

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# MAX_CONCURRENT_REQUESTS = 1
# tender_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

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

# @app.post("/process/{tender_id}")
# async def route_process(tender_id: str):
#     tender_id = str(tender_id)
#     async with tender_semaphore:  # only 4 requests can run concurrently
#         try:
#             report = await process_single_tender(tender_id)
#             return report
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=str(e))
