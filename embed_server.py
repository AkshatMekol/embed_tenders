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
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PDF_BATCH_SIZE = 20
GPU_SERVER_URL = "http://127.0.0.1:9000/enqueue"  # GPU server endpoint

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

    s3_prefix = f"tender-documents/{tender_id}/"
    pdf_keys = await list_s3_pdfs(s3_prefix)

    for pdf_key in pdf_keys:
        document_name = os.path.basename(pdf_key)

        existing = await asyncio.to_thread(
            vector_collection.find_one,
            {"tender_id": tender_id, "document_name": document_name},
            {"document_complete": 1}
        )

        if existing and existing.get("document_complete"):
            report["skipped_docs"] += 1
            continue

        if existing:
            await asyncio.to_thread(
                vector_collection.delete_many,
                {"tender_id": tender_id, "document_name": document_name}
            )

        try:
            pdf_stream = await fetch_pdf(pdf_key)
            pdf_bytes = pdf_stream.read()

            total_pages = await asyncio.to_thread(
                lambda: len(pdfplumber.open(BytesIO(pdf_bytes)).pages)
            )
            if total_pages == 0:
                report["empty_docs"] += 1
                continue

            for start in range(0, total_pages, PDF_BATCH_SIZE):
                end = start + PDF_BATCH_SIZE
                is_last = (end >= total_pages)

                chunks, scanned, regular = await process_pdf_batch(pdf_bytes, start, end)

                report["scanned_pages"] += scanned
                report["regular_pages"] += regular

                if chunks:
                    # Send to GPU server
                    requests.post(GPU_SERVER_URL, json={
                        "chunks": chunks,
                        "document_name": document_name,
                        "tender_id": tender_id,
                        "is_last_batch": is_last
                    })

            report["processed_docs"] += 1

        except Exception as e:
            report["errors"].append(str(e))

    return report

@app.post("/process/{tender_id}")
async def route_process(tender_id: str):
    try:
        return await process_single_tender(tender_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
