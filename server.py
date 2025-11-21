from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os
import gc
from io import BytesIO
import asyncio
from fastapi import FastAPI, HTTPException
from utils.embedding_client import EmbeddingClient
from utils.pdf_processing import process_pdf_batch
from utils.mongo_utils import vector_collection
from utils.s3_utils import list_s3_pdfs, fetch_pdf
import pdfplumber

# Embedding client (initialized in lifespan)
embedding_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler for FastAPI."""
    global embedding_client

    # Startup - connect to embedding server
    try:
        embedding_client = EmbeddingClient("http://localhost:9000")
        # Test connection
        health = embedding_client.health_check()
        if health.get("status") == "healthy":
            print("✅ Connected to GPU Embedding service")
        else:
            print(f"⚠️ Embedding server health check failed: {health}")
            embedding_client = None
    except Exception as e:
        print(f"⚠️ Failed to connect to embedding server: {e}")
        print("💡 Make sure embedding server is running on port 9000")
        embedding_client = None

    yield

    # Shutdown
    print("✅ Shutdown complete")


app = FastAPI(lifespan=lifespan)


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


async def process_single_tender_cpu(tender_id: str):
    print(f"[{tender_id}] Starting tender processing (CPU process)...")

    report = {
        "tender_id": tender_id,
        "processed_docs": 0,
        "skipped_docs": 0,
        "empty_docs": 0,
        "scanned_pages": 0,
        "regular_pages": 0,
        "errors": [],
    }

    try:
        s3_prefix = f"tender-documents/{tender_id}/"
        pdf_keys = await list_s3_pdfs(s3_prefix)
        print(f"[{tender_id}] Found {len(pdf_keys)} PDF(s) in S3")

        for pdf_key in pdf_keys:
            document_name = os.path.basename(pdf_key)
            print(f"[{tender_id}] Processing document: {document_name}")

            # Check document_complete
            doc_entry = vector_collection.find_one(
                {"tender_id": tender_id, "document_name": document_name},
                {"document_complete": 1},
            )
            if doc_entry and doc_entry.get("document_complete") is True:
                print(
                    f"[{tender_id}] Skipping already completed document: {document_name}"
                )
                report["skipped_docs"] += 1
                continue
            elif doc_entry:
                print(f"[{tender_id}] Deleting partial embeddings for {document_name}")
                vector_collection.delete_many(
                    {"tender_id": tender_id, "document_name": document_name}
                )

            try:
                pdf_stream = await fetch_pdf(pdf_key)
                pdf_bytes = pdf_stream.read()
                print(
                    f"[{tender_id}] Fetched PDF: {document_name} ({len(pdf_bytes)} bytes)"
                )

                # Convert to seekable object
                pdf_io = BytesIO(pdf_bytes)

                with pdfplumber.open(pdf_io) as pdf:
                    total_pages = len(pdf.pages)
                print(f"[{tender_id}] Total pages in {document_name}: {total_pages}")

                # Process in batches
                for start in range(0, total_pages, PDF_BATCH_SIZE):
                    end = start + PDF_BATCH_SIZE
                    print(
                        f"[{tender_id}] Processing pages {start} to {end} of {document_name}"
                    )
                    chunks, scanned_count, regular_count = await process_pdf_batch(
                        pdf_bytes, start, end
                    )

                    report["scanned_pages"] += scanned_count
                    report["regular_pages"] += regular_count

                    if chunks:
                        print(
                            f"[{tender_id}] Queueing {len(chunks)} chunks for GPU embedding"
                        )
                        # Enqueue task using embedding client
                        if embedding_client is None:
                            print(
                                f"⚠️ Embedding client not initialized. Skipping embedding for {document_name}"
                            )
                            report["errors"].append(
                                f"{document_name}: Embedding client not available"
                            )
                        else:
                            try:
                                embedding_client.enqueue_task(
                                    chunks, document_name, tender_id
                                )
                            except Exception as e:
                                print(f"⚠️ Failed to enqueue task: {e}")
                                report["errors"].append(f"{document_name}: {str(e)}")
                        gc.collect()

                # Mark complete
                vector_collection.update_one(
                    {"tender_id": tender_id, "document_name": document_name},
                    {"$set": {"document_complete": True}},
                    upsert=True,
                )
                report["processed_docs"] += 1
                print(f"[{tender_id}] Finished document: {document_name}")
                del pdf_stream, pdf_bytes
                gc.collect()

            except Exception as e:
                print(f"[{tender_id}] Error processing {document_name}: {e}")
                report["errors"].append(f"{document_name}: {str(e)}")

    except Exception as e:
        print(f"[{tender_id}] Unexpected error: {e}")
        report["errors"].append(str(e))

    print(f"[{tender_id}] Tender processing complete")
    return report


# --------------------------
# FastAPI route: runs in process pool
# --------------------------
tender_semaphore = asyncio.Semaphore(4)


@app.post("/process/{tender_id}")
async def route_process(tender_id: str):
    tender_id = str(tender_id)
    print(f"[{tender_id}] Received POST request")

    async with tender_semaphore:
        import os

        print(
            f"[{tender_id}] Acquired semaphore (pid {os.getpid()}), submitting to process pool"
        )
        try:
            report = await process_single_tender_cpu(tender_id)
            print(f"[{tender_id}] Returned report from process pool")
            return report
        except Exception as e:
            print(f"[{tender_id}] Exception in route: {e}")
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
#         "errors": [],
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
#                 {"tender_id": tender_id, "document_name": document_name},
#             )
#             if doc_exists > 0:
#                 print(
#                     f"[{tender_id}] Skipping already processed document: {document_name}"
#                 )
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

#                 print(
#                     f"[{tender_id}] Queueing {len(chunks)} chunks for embedding: {document_name}"
#                 )
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
