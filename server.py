from fastapi.middleware.cors import CORSMiddleware
import os
import gc
import asyncio
import requests
import pdfplumber
from io import BytesIO
from fastapi import FastAPI, HTTPException
from utils.s3_utils import list_s3_pdfs, fetch_pdf
from utils.pdf_processing import process_pdf_batch
from utils.mongo_utils import vector_collection, is_document_complete

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

async def process_single_tender(tender_id: str):
    print(f"\n===============================")
    print(f"▶ START tender: {tender_id}")
    print(f"===============================")

    report = {
        "tender_id": tender_id,
        "processed_docs": 0,
        "skipped_docs": 0,
        "scanned_pages": 0,
        "regular_pages": 0,
        "total_chunks": 0,
        "errors": []
    }

    s3_prefix = f"tender-documents/{tender_id}/"
    print(f"📂 Fetching S3 PDFs from prefix: {s3_prefix}")

    pdf_keys = await list_s3_pdfs(s3_prefix)
    print(f"📄 Found {len(pdf_keys)} PDFs")

    for pdf_key in pdf_keys:
        document_name = os.path.basename(pdf_key)
        print(f"📄 Document: {document_name}")

        if await asyncio.to_thread(is_document_complete, tender_id, document_name):
            print(f"⏩ Already processed, skipping")
            report["skipped_docs"] += 1
            continue

        await asyncio.to_thread(
            vector_collection.delete_many,
            {"tender_id": tender_id, "document_name": document_name}
        )
        print("🗑 Removed previous embeddings (if any)")

        try:
            print("⬇ Fetching PDF from S3")
            pdf_stream = await fetch_pdf(pdf_key)
            pdf_bytes = pdf_stream.read()

            scanned, regular, total_chunks = await process_pdf(pdf_bytes, document_name, tender_id)

            report["scanned_pages"] += scanned
            report["regular_pages"] += regular
            report["total_chunks"] += total_chunks
            report["processed_docs"] += 1

            print(f"✔ Completed PDF: {document_name} | Scanned={scanned}, Regular={regular}, Chunks={total_chunks}")

        except Exception as e:
            print(f"❌ Error processing {document_name}: {e}")
            report["errors"].append(f"{document_name}: {str(e)}")

    print(f"\n🎯 Tender {tender_id} COMPLETED\n")
    return report

@app.post("/process/{tender_id}")
async def route_process(tender_id: str):
    print(f"\n🌐 API CALL → /process/{tender_id}")
    try:
        return await process_single_tender(tender_id)
    except Exception as e:
        print(f"❌ API ERROR: {e}")
        raise HTTPException(status_code=500, detail=str(e))
