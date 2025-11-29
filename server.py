from fastapi.middleware.cors import CORSMiddleware
import os
import gc
import asyncio
import requests
import pdfplumber
from io import BytesIO
from fastapi import FastAPI, HTTPException
from utils.embedding_utils import embed_batch 
from utils.s3_utils import list_s3_pdfs, fetch_pdf
from utils.pdf_processing import process_pdf_batch
from utils.mongo_utils import vector_collection, is_document_complete, store_embeddings_in_db, mark_document_complete

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

            total_pages = await asyncio.to_thread(
                lambda: len(pdfplumber.open(BytesIO(pdf_bytes)).pages)
            )
            print(f"📄 Total pages: {total_pages}")

            if total_pages == 0:
                print("⚠ Empty PDF, skipping")
                report["empty_docs"] += 1
                continue

            file_size_kb = len(pdf_bytes) / 1024
            size_per_page_kb = file_size_kb / max(total_pages, 1)
            if size_per_page_kb < 250:
                batch_size = 20
            else:
                batch_size = 5
            print(f"📦 Dynamic batch size = {batch_size} (size_per_page={size_per_page_kb:.1f} KB)")

            for start in range(0, total_pages, batch_size):
                end = min(start + batch_size, total_pages)
                is_last = (end >= total_pages)
                print(f"🔹 Page batch: {start} → {end} (last={is_last})")

                chunks, scanned, regular = await process_pdf_batch(
                    pdf_bytes, start, end
                )

                print(f"   • Chunks = {len(chunks)} | Scanned = {scanned} | Regular = {regular}")

                report["scanned_pages"] += scanned
                report["regular_pages"] += regular

                if chunks:
                    try:
                        for c in chunks:
                            c["tender_id"] = tender_id
                            c["document_name"] = document_name
                
                        embeddings = await asyncio.to_thread(embed_batch, chunks)
                        await asyncio.to_thread(store_embeddings_in_db, embeddings, document_name, tender_id)
                        print(f"[{document_name}] 🔹 Batch embedded & stored ({len(chunks)} chunks)")
                
                        if is_last:
                            await asyncio.to_thread(mark_document_complete, tender_id, document_name)
                            print(f"[{document_name}] 🎉 Document marked COMPLETE")
                
                    except Exception as e:
                        print(f"❌ Error embedding batch: {e}")
                        report["errors"].append(f"{document_name}: {str(e)}")
                
                del chunks
                gc.collect()

            print(f"✔ Completed queuing document: {document_name}")
            report["processed_docs"] += 1

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
