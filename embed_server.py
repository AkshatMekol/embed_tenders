from fastapi.middleware.cors import CORSMiddleware
import os
import gc
from io import BytesIO
import asyncio
from fastapi import FastAPI, HTTPException
from utils.pdf_processing import process_pdf_batch
from utils.mongo_utils import vector_collection
from utils.s3_utils import list_s3_pdfs, fetch_pdf
import pdfplumber
import requests

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

PDF_BATCH_SIZE = 10
GPU_SERVER_URL = "http://127.0.0.1:9000/enqueue"

# =======================================================
# MAIN WORKER LOGIC
# =======================================================
async def process_single_tender(tender_id: str):
    print(f"\n===============================")
    print(f"▶ START tender: {tender_id}")
    print(f"===============================")

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
    print(f"📂 Fetching S3 PDFs from prefix: {s3_prefix}")

    pdf_keys = await list_s3_pdfs(s3_prefix)
    print(f"📄 Found {len(pdf_keys)} PDFs")

    for pdf_key in pdf_keys:
        document_name = os.path.basename(pdf_key)
        print(f"\n--------------------------------")
        print(f"📄 Document: {document_name}")
        print(f"--------------------------------")

        # Check if already completed
        existing = await asyncio.to_thread(
            vector_collection.find_one,
            {"tender_id": tender_id, "document_name": document_name},
            {"document_complete": 1}
        )

        if existing and existing.get("document_complete"):
            print(f"⏩ Already processed, skipping")
            report["skipped_docs"] += 1
            continue

        if existing:
            print("🗑 Removing partial embeddings from MongoDB...")
            await asyncio.to_thread(
                vector_collection.delete_many,
                {"tender_id": tender_id, "document_name": document_name}
            )

        try:
            print("⬇ Fetching PDF from S3")
            pdf_stream = await fetch_pdf(pdf_key)
            pdf_bytes = pdf_stream.read()

            total_pages = await asyncio.to_thread(
                lambda: len(pdfplumber.open(BytesIO(pdf_bytes)).pages)
            )
            print(f"📄 Total pages: {total_pages}")

            if total_pages == 0:
                print("⚠ Empty PDF, skipping")
                report["empty_docs"] += 1
                continue

            # Process in batches
            for start in range(0, total_pages, PDF_BATCH_SIZE):
                end = min(start + PDF_BATCH_SIZE, total_pages)
                is_last = (end >= total_pages)

                print(f"🔹 Page batch: {start} → {end} (last={is_last})")

                chunks, scanned, regular = await process_pdf_batch(
                    pdf_bytes, start, end
                )

                print(f"   • Chunks = {len(chunks)} | Scanned = {scanned} | Regular = {regular}")

                report["scanned_pages"] += scanned
                report["regular_pages"] += regular

                if chunks:
                    print("   → Sending batch to GPU server...")
                    try:
                        resp = requests.post(GPU_SERVER_URL, json={
                            "chunks": chunks,
                            "document_name": document_name,
                            "tender_id": tender_id,
                            "is_last_batch": is_last
                        })
                        print(f"     GPU Response: {resp.status_code}")
                    except Exception as e:
                        print(f"❌ GPU enqueue failed: {e}")
                        report["errors"].append(f"{document_name}: GPU error - {str(e)}")

                del chunks
                resp.close()
                del resp
                gc.collect()

            print(f"✔ Completed queuing document: {document_name}")
            report["processed_docs"] += 1

        except Exception as e:
            print(f"❌ Error processing {document_name}: {e}")
            report["errors"].append(f"{document_name}: {str(e)}")

    print(f"\n🎯 Tender {tender_id} COMPLETED\n")
    return report


# =======================================================
# API ENDPOINT
# =======================================================
@app.post("/process/{tender_id}")
async def route_process(tender_id: str):
    print(f"\n🌐 API CALL → /process/{tender_id}")
    try:
        return await process_single_tender(tender_id)
    except Exception as e:
        print(f"❌ API ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))
