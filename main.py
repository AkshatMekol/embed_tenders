import asyncio
from tabulate import tabulate
from tender_embedding.s3_utils import list_pdfs, download_pdf
from tender_embedding.pdf_utils import process_pdf_to_subchunks
from tender_embedding.embed_utils import embed_texts
from tender_embedding.mongo_utils import get_tender_ids, insert_vectors, folder_exists_for_tender

MIN_TENDER_VALUE = 1_000_000_000
MAX_CONCURRENT_TENDERS = 2
BATCH_SIZE = 512

sem = asyncio.Semaphore(MAX_CONCURRENT_TENDERS)

async def process_single_tender(tender_id):
    async with sem:
        print(f"[{tender_id}] 🚀 Starting tender processing")
        report = []
        try:
            s3_prefix = f"tender-documents/{tender_id}/"
            pdf_keys = await list_pdfs(s3_prefix)
            print(f"[{tender_id}] Found {len(pdf_keys)} PDFs")

            for key in pdf_keys:
                document_name = key.split("/")[-1]
                folder_name = key.split("/")[1]  

                already_exists = await folder_exists_for_tender(tender_id, document_name)
                if already_exists:
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
                print(f"[{tender_id}] ✅ Embeddings generated successfully")

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
            print(f"[{tender_id}] ❌ Error during processing: {e}")

        print(f"[{tender_id}] 🏁 Finished processing\n")
        return tender_id, report

async def main():
    print("🔍 Fetching tender IDs from MongoDB...")
    tender_ids = await get_tender_ids(MIN_TENDER_VALUE)
    print(f"📦 Found {len(tender_ids)} tenders above {MIN_TENDER_VALUE}")

    print("⚙️ Starting concurrent tender processing...\n")
    tasks = [process_single_tender(tid) for tid in tender_ids]
    results = await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
