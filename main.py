import asyncio
from multiprocessing import Pool, cpu_count
from utils.s3_utils import list_pdfs, download_pdf
from utils.pdf_utils import process_pdf_to_subchunks
from utils.embed_utils import embed_texts
from utils.mongo_utils import get_tender_ids, insert_vectors, folder_exists_for_tender

# ==============================
# CONFIG
# ==============================
MIN_TENDER_VALUE = 1_000_000_000
MAX_CONCURRENT_TENDERS = 4     # async concurrency within each process
BATCH_SIZE = 512               # embedding batch size
USE_CORES = cpu_count()        # use all available cores

# ==============================
# ASYNC TENDER PROCESSING
# ==============================
async def process_single_tender(tender_id, sem):
    async with sem:
        print(f"[{tender_id}] 🚀 Starting processing")
        try:
            s3_prefix = f"tender-documents/{tender_id}/"
            pdf_keys = await list_pdfs(s3_prefix)
            print(f"[{tender_id}] Found {len(pdf_keys)} PDFs")

            for key in pdf_keys:
                document_name = key.split("/")[-1]
                folder_name = key.split("/")[1]

                # Skip if already exists in Mongo
                if await folder_exists_for_tender(tender_id, document_name):
                    print(f"[{tender_id}] ⚠️ {document_name} already exists, skipping.")
                    continue

                print(f"[{tender_id}] 📄 Downloading {document_name}")
                pdf_stream = await download_pdf(key)

                print(f"[{tender_id}] ✂️ Splitting {document_name} into sub-chunks")
                sub_chunks = process_pdf_to_subchunks(pdf_stream, document_name)
                if not sub_chunks:
                    print(f"[{tender_id}] ⚠️ {document_name} has zero chunks, skipping")
                    continue

                texts = [sc["data"] for sc in sub_chunks]
                embeddings = embed_texts(texts, batch_size=BATCH_SIZE)
                print(f"[{tender_id}] ✅ Embeddings generated")

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
                await insert_vectors(docs)
                print(f"[{tender_id}] ✅ Uploaded {document_name} successfully")

        except Exception as e:
            print(f"[{tender_id}] ❌ Error: {e}")

        print(f"[{tender_id}] 🏁 Finished processing\n")
        return tender_id

async def process_tenders_in_process(tender_ids):
    sem = asyncio.Semaphore(MAX_CONCURRENT_TENDERS)
    tasks = [process_single_tender(tid, sem) for tid in tender_ids]
    await asyncio.gather(*tasks)

# ==============================
# MULTIPROCESS WRAPPER
# ==============================
def process_batch(tender_ids):
    """
    Each process runs its own asyncio event loop
    and handles a batch of tenders.
    """
    try:
        asyncio.run(process_tenders_in_process(tender_ids))
    except Exception as e:
        print(f"⚠️ Process batch failed: {e}")

# ==============================
# MAIN ENTRY POINT
# ==============================
def main():
    print("🔍 Fetching tender IDs from MongoDB...")
    tender_ids = asyncio.run(get_tender_ids(MIN_TENDER_VALUE))
    print(f"📦 Found {len(tender_ids)} tenders above {MIN_TENDER_VALUE}\n")

    if not tender_ids:
        print("No tenders found — exiting.")
        return

    n_cores = min(USE_CORES, len(tender_ids))
    print(f"🧠 Using {n_cores} CPU cores for parallel processing")

    # Split tender IDs evenly across cores
    chunk_size = (len(tender_ids) + n_cores - 1) // n_cores
    tender_batches = [
        tender_ids[i:i + chunk_size] for i in range(0, len(tender_ids), chunk_size)
    ]

    print(f"📦 Split into {len(tender_batches)} batches of ~{chunk_size} tenders each\n")

    with Pool(n_cores) as pool:
        pool.map(process_batch, tender_batches)

    print("\n✅ All tenders processed successfully!")

# ==============================
# RUN SCRIPT
# ==============================
if __name__ == "__main__":
    main()

