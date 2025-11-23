# embed_server.py
import os
import gc
from io import BytesIO
import asyncio
from typing import List
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from utils.pdf_processing import process_pdf_batch
from utils.mongo_utils import vector_collection
from utils.s3_utils import list_s3_pdfs, fetch_pdf
import pdfplumber
import aiohttp

app = FastAPI()

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

# === CONFIG ===
PDF_BATCH_SIZE = int(os.environ.get("PDF_BATCH_SIZE", 20))
GPU_SERVER_URL = os.environ.get("GPU_SERVER_URL", "http://127.0.0.1:9000/enqueue")
GPU_POST_TIMEOUT = int(os.environ.get("GPU_POST_TIMEOUT", 30))  # seconds

# Helper: open pdfplumber from stream in a thread
def _open_pdf_from_stream(stream_like):
    try:
        stream_like.seek(0)
    except Exception:
        pass
    return pdfplumber.open(stream_like)

# Helper: flush page caches for memory efficiency
def _flush_pdf_page_cache(pdf):
    try:
        for p in pdf.pages:
            for attr in ("chars", "objects", "_objects", "rects", "images", "debug_table"):
                if hasattr(p, attr):
                    try:
                        val = getattr(p, attr)
                        if isinstance(val, list):
                            val.clear()
                        else:
                            setattr(p, attr, None)
                    except Exception:
                        pass
    except Exception:
        pass

# Helper: send all chunks asynchronously
async def _async_send_chunks(session: aiohttp.ClientSession, chunks: List[dict], document_name: str, tender_id: str, is_last_batch: bool):
    try:
        payload = {
            "chunks": chunks,
            "document_name": document_name,
            "tender_id": tender_id,
            "is_last_batch": is_last_batch
        }
        async with session.post(GPU_SERVER_URL, json=payload, timeout=GPU_POST_TIMEOUT) as resp:
            status = resp.status
            text = await resp.text()
            if status >= 400:
                return f"Error {status}: {text}"
            return None
    except Exception as e:
        return str(e)

# ========== main processing ==========
async def process_single_tender(tender_id: str):
    print(f"\n===============================", flush=True)
    print(f"▶ START tender: {tender_id}", flush=True)
    print(f"===============================", flush=True)

    report = {
        "tender_id": tender_id,
        "processed_docs": 0,
        "skipped_docs": 0,
        "empty_docs": 0,
        "scanned_pages": 0,
        "regular_pages": 0,
        "errors": []
    }

    s3_prefix = f"tender-documents/{tender_id}/"
    print(f"📂 Fetching S3 PDFs from prefix: {s3_prefix}", flush=True)

    pdf_keys = await list_s3_pdfs(s3_prefix)
    print(f"📄 Found {len(pdf_keys)} PDFs", flush=True)

    async with aiohttp.ClientSession() as session:
        for pdf_key in pdf_keys:
            document_name = os.path.basename(pdf_key)
            print("\n--------------------------------", flush=True)
            print(f"📄 Document: {document_name}", flush=True)
            print("--------------------------------", flush=True)

            existing = await asyncio.to_thread(
                vector_collection.find_one,
                {"tender_id": tender_id, "document_name": document_name},
                {"document_complete": 1}
            )

            if existing and existing.get("document_complete"):
                print(f"⏩ Already processed, skipping", flush=True)
                report["skipped_docs"] += 1
                continue

            if existing:
                print("🗑 Removing partial embeddings from MongoDB...", flush=True)
                await asyncio.to_thread(
                    vector_collection.delete_many,
                    {"tender_id": tender_id, "document_name": document_name}
                )

            try:
                print("⬇ Fetching PDF from S3 (stream)", flush=True)
                pdf_stream = await fetch_pdf(pdf_key)
                if isinstance(pdf_stream, (bytes, bytearray)):
                    pdf_stream = BytesIO(pdf_stream)

                pdf = await asyncio.to_thread(_open_pdf_from_stream, pdf_stream)

                try:
                    total_pages = len(pdf.pages)
                except Exception as e:
                    print(f"❌ Failed to read pages for {document_name}: {e}", flush=True)
                    report["errors"].append(f"{document_name}: read pages error - {e}")
                    pdf.close()
                    continue

                print(f"📄 Total pages: {total_pages}", flush=True)
                if total_pages == 0:
                    print("⚠ Empty PDF, skipping", flush=True)
                    report["empty_docs"] += 1
                    pdf.close()
                    continue

                for start in range(0, total_pages, PDF_BATCH_SIZE):
                    end = min(start + PDF_BATCH_SIZE, total_pages)
                    is_last = (end >= total_pages)

                    print(f"🔹 Page batch: {start} → {end} (last={is_last})", flush=True)

                    chunks, scanned, regular = await process_pdf_batch(pdf=pdf, start_page=start, end_page=end)

                    print(f"   • Chunks = {len(chunks)} | Scanned = {scanned} | Regular = {regular}", flush=True)

                    report["scanned_pages"] += scanned
                    report["regular_pages"] += regular

                    if chunks:
                        error = await _async_send_chunks(session, chunks, document_name, tender_id, is_last)
                        if error:
                            report["errors"].append(f"{document_name}: GPU enqueue error - {error}")
                            print(f"❌ GPU enqueue error: {error}", flush=True)

                    _flush_pdf_page_cache(pdf)
                    gc.collect()
                    await asyncio.sleep(0)

                print(f"✔ Completed queuing document: {document_name}", flush=True)
                report["processed_docs"] += 1

                pdf.close()
                pdf_stream.close()
                del pdf
                del pdf_stream
                gc.collect()

            except Exception as e:
                print(f"❌ Error processing {document_name}: {e}", flush=True)
                report["errors"].append(f"{document_name}: {str(e)}")
                try:
                    pdf.close()
                except Exception:
                    pass
                try:
                    pdf_stream.close()
                except Exception:
                    pass
                gc.collect()

    print(f"\n🎯 Tender {tender_id} COMPLETED\n", flush=True)
    return report

# =======================================================
# API ENDPOINT
# =======================================================
@app.post("/process/{tender_id}")
async def route_process(tender_id: str):
    print(f"\n🌐 API CALL → /process/{tender_id}", flush=True)
    try:
        return await process_single_tender(tender_id)
    except Exception as e:
        print(f"❌ API ERROR: {e}", flush=True)
        raise HTTPException(status_code=500, detail=str(e))


# from fastapi.middleware.cors import CORSMiddleware
# import os
# import gc
# from io import BytesIO
# import asyncio
# from fastapi import FastAPI, HTTPException
# from utils.pdf_processing import process_pdf_batch
# from utils.mongo_utils import vector_collection
# from utils.s3_utils import list_s3_pdfs, fetch_pdf
# import pdfplumber
# import requests

# app = FastAPI()

# origins = [
#     "http://localhost:8080",
#     "http://192.168.1.5:8080",
#     "https://tenderbharat.vercel.app",
#     "http://localhost:3000",
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

# PDF_BATCH_SIZE = 20
# GPU_SERVER_URL = "http://127.0.0.1:9000/enqueue"

# # =======================================================
# # MAIN WORKER LOGIC
# # =======================================================
# async def process_single_tender(tender_id: str):
#     print(f"\n===============================")
#     print(f"▶ START tender: {tender_id}")
#     print(f"===============================")

#     report = {
#         "tender_id": tender_id,
#         "processed_docs": 0,
#         "skipped_docs": 0,
#         "empty_docs": 0,
#         "scanned_pages": 0,
#         "regular_pages": 0,
#         "errors": []
#     }

#     s3_prefix = f"tender-documents/{tender_id}/"
#     print(f"📂 Fetching S3 PDFs from prefix: {s3_prefix}")

#     pdf_keys = await list_s3_pdfs(s3_prefix)
#     print(f"📄 Found {len(pdf_keys)} PDFs")

#     for pdf_key in pdf_keys:
#         document_name = os.path.basename(pdf_key)
#         print(f"\n--------------------------------")
#         print(f"📄 Document: {document_name}")
#         print(f"--------------------------------")

#         # Check if already completed
#         existing = await asyncio.to_thread(
#             vector_collection.find_one,
#             {"tender_id": tender_id, "document_name": document_name},
#             {"document_complete": 1}
#         )

#         if existing and existing.get("document_complete"):
#             print(f"⏩ Already processed, skipping")
#             report["skipped_docs"] += 1
#             continue

#         if existing:
#             print("🗑 Removing partial embeddings from MongoDB...")
#             await asyncio.to_thread(
#                 vector_collection.delete_many,
#                 {"tender_id": tender_id, "document_name": document_name}
#             )

#         try:
#             print("⬇ Fetching PDF from S3")
#             pdf_stream = await fetch_pdf(pdf_key)
#             pdf_bytes = pdf_stream.read()

#             total_pages = await asyncio.to_thread(
#                 lambda: len(pdfplumber.open(BytesIO(pdf_bytes)).pages)
#             )
#             print(f"📄 Total pages: {total_pages}")

#             if total_pages == 0:
#                 print("⚠ Empty PDF, skipping")
#                 report["empty_docs"] += 1
#                 continue

#             # Process in batches
#             for start in range(0, total_pages, PDF_BATCH_SIZE):
#                 end = min(start + PDF_BATCH_SIZE, total_pages)
#                 is_last = (end >= total_pages)

#                 print(f"🔹 Page batch: {start} → {end} (last={is_last})")

#                 chunks, scanned, regular = await process_pdf_batch(
#                     pdf_bytes, start, end
#                 )

#                 print(f"   • Chunks = {len(chunks)} | Scanned = {scanned} | Regular = {regular}")

#                 report["scanned_pages"] += scanned
#                 report["regular_pages"] += regular

#                 if chunks:
#                     print("   → Sending batch to GPU server...")
#                     try:
#                         resp = requests.post(GPU_SERVER_URL, json={
#                             "chunks": chunks,
#                             "document_name": document_name,
#                             "tender_id": tender_id,
#                             "is_last_batch": is_last
#                         })
#                         print(f"     GPU Response: {resp.status_code}")
#                     except Exception as e:
#                         print(f"❌ GPU enqueue failed: {e}")
#                         report["errors"].append(f"{document_name}: GPU error - {str(e)}")

#                 gc.collect()

#             print(f"✔ Completed queuing document: {document_name}")
#             report["processed_docs"] += 1

#         except Exception as e:
#             print(f"❌ Error processing {document_name}: {e}")
#             report["errors"].append(f"{document_name}: {str(e)}")

#     print(f"\n🎯 Tender {tender_id} COMPLETED\n")
#     return report


# # =======================================================
# # API ENDPOINT
# # =======================================================
# @app.post("/process/{tender_id}")
# async def route_process(tender_id: str):
#     print(f"\n🌐 API CALL → /process/{tender_id}")
#     try:
#         return await process_single_tender(tender_id)
#     except Exception as e:
#         print(f"❌ API ERROR: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
