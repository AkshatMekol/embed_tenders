import asyncio
import traceback
from datetime import datetime
from tabulate import tabulate
from utils.s3_utils import list_pdfs, download_pdf
from utils.pdf_utils import process_pdf_to_subchunks
from utils.embed_utils import embed_texts
from utils.mongo_utils import get_tender_ids, insert_vectors, folder_exists_for_tender

# ==== CONFIG ====
MIN_TENDER_VALUE = 1_000_000_000
MAX_CONCURRENT_TENDERS = 4   # increase this for higher utilization
BATCH_SIZE = 512

# === SEMAPHORE FOR CONCURRENCY CONTROL ===
sem = asyncio.Semaphore(MAX_CONCURRENT_TENDERS)

async def process_single_tender(tender_id):
    """Process one tender and all its PDFs"""
    async with sem:
        start_time = datetime.now()
        report_rows = []

        print(f"\n[{tender_id}] 🚀 Starting processing at {start_time.strftime('%H:%M:%S')}")

        try:
            s3_prefix = f"tender-documents/{tender_id}/"
            pdf_keys = await list_pdfs(s3_prefix)
            print(f"[{tender_id}] 📦 Found {len(pdf_keys)} PDFs in S3")

            if not pdf_keys:
                report_rows.append([tender_id, "❌ No PDFs found", "-", "-", "0 sec"])
                return tender_id, report_rows

            for key in pdf_keys:
                document_name = key.split("/")[-1]
                folder_name = key.split("/")[1]

                try:
                    # === Check existence ===
                    if await folder_exists_for_tender(tender_id, document_name):
                        print(f"[{tender_id}] ⚠️ {document_name} already exists, skipping.")
                        report_rows.append([tender_id, document_name, "Skipped", "Already Exists", "-"])
                        continue

                    # === Download PDF ===
                    print(f"[{tender_id}] ⬇️ Downloading {document_name}")
                    pdf_stream = await download_pdf(key)

                    # === Process into chunks ===
                    print(f"[{tender_id}] ✂️ Splitting {document_name}")
                    sub_chunks = process_pdf_to_subchunks(pdf_stream, document_name)
                    if not sub_chunks:
                        print(f"[{tender_id}] ⚠️ No subchunks found for {document_name}")
                        report_rows.append([tender_id, document_name, "Skipped", "Empty", "-"])
                        continue

                    # === Generate embeddings ===
                    texts = [sc["data"] for sc in sub_chunks]
                    embeddings = embed_texts(texts, batch_size=BATCH_SIZE)
                    print(f"[{tender_id}] 🧠 Generated {len(embeddings)} embeddings")

                    # === Prepare Mongo docs ===
                    docs = [
                        {
                            "tender_id": tender_id,
                            "folder_name": folder_name,
                            "document_name": sc["document_name"],
                            "page": sc["page"],
                            "position": sc["position"],
                            "sub_position": sc["sub_position"],
                            "type": sc["type"],
                            "text": sc["data"],
                            "embedding": emb,
                        }
                        for sc, emb in zip(sub_chunks, embeddings)
                    ]

                    # === Upload to Mongo ===
                    await insert_vectors(docs)
                    report_rows.append([tender_id, document_name, "✅ Success", f"{len(sub_chunks)} chunks", "-"])
                    print(f"[{tender_id}] ✅ Uploaded {document_name} successfully")

                except Exception as doc_err:
                    err_trace = traceback.format_exc(limit=1)
                    print(f"[{tender_id}] ❌ Error in {document_name}: {doc_err}")
                    report_rows.append([tender_id, document_name, "❌ Failed", str(doc_err)[:60], "-"])

        except Exception as e:
            err_trace = traceback.format_exc(limit=1)
            print(f"[{tender_id}] 🚨 Fatal tender-level error: {e}\n{err_trace}")
            report_rows.append([tender_id, "All Docs", "❌ Tender Failed", str(e)[:60], "-"])

        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        print(f"[{tender_id}] 🏁 Finished in {duration} sec\n")

        return tender_id, report_rows


async def main():
    print("🔍 Fetching tender IDs from MongoDB...")
    tender_ids = await get_tender_ids(MIN_TENDER_VALUE)
    print(f"📦 Found {len(tender_ids)} tenders above {MIN_TENDER_VALUE}\n")

    print(f"⚙️ Starting processing with {MAX_CONCURRENT_TENDERS} concurrent tenders...\n")

    # === Run all tenders concurrently ===
    tasks = [process_single_tender(tid) for tid in tender_ids]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # === Flatten and summarize results ===
    all_reports = []
    for tid, rows in results:
        all_reports.extend(rows)

    print("\n📊 FINAL REPORT:")
    print(tabulate(all_reports, headers=["Tender ID", "Document", "Status", "Details", "Duration"], tablefmt="grid"))


if __name__ == "__main__":
    asyncio.run(main())
