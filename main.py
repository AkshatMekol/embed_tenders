import asyncio
from tqdm.asyncio import tqdm_asyncio
from utils.s3_utils import list_pdfs, download_pdf
from utils.pdf_utils import process_pdf_to_subchunks
from utils.embed_utils import embed_texts
from utils.mongo_utils import get_tender_ids, insert_vectors, folder_exists_for_tender

MIN_TENDER_VALUE = 1_000_000_000
MAX_CONCURRENT_TENDERS = 2
BATCH_SIZE = 512

sem = asyncio.Semaphore(MAX_CONCURRENT_TENDERS)

async def process_single_tender(tender_id):
    async with sem:
        try:
            s3_prefix = f"tender-documents/{tender_id}/"
            pdf_keys = await list_pdfs(s3_prefix)
            if not pdf_keys:
                return tender_id, 0  # no PDFs

            for key in pdf_keys:
                document_name = key.split("/")[-1]
                folder_name = key.split("/")[1]

                if await folder_exists_for_tender(tender_id, document_name):
                    continue

                pdf_stream = await download_pdf(key)
                sub_chunks = process_pdf_to_subchunks(pdf_stream, document_name)
                if not sub_chunks:
                    continue

                texts = [sc["data"] for sc in sub_chunks]
                embeddings = embed_texts(texts, batch_size=BATCH_SIZE)

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

        except Exception as e:
            # You can log exceptions somewhere if needed
            return tender_id, 0

        return tender_id, 1  # tender processed successfully

async def main():
    print("🔍 Fetching tender IDs from MongoDB...")
    tender_ids = await get_tender_ids(MIN_TENDER_VALUE)
    print(f"📦 Found {len(tender_ids)} tenders above {MIN_TENDER_VALUE}\n")

    print("⚙️ Starting concurrent tender processing...\n")
    tasks = [process_single_tender(tid) for tid in tender_ids]

    # Use tqdm_asyncio to track progress
    results = []
    for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Processing tenders"):
        res = await f
        results.append(res)

    print("\n✅ All tenders processed!")

if __name__ == "__main__":
    asyncio.run(main())
