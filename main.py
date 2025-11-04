import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

from utils.s3_utils import list_pdfs_sync, download_pdf_sync
from utils.pdf_utils import process_pdf_to_subchunks
from utils.embed_utils import embed_texts
from utils.mongo_utils import get_tender_ids_sync, insert_vectors_sync, folder_exists_for_tender_sync

MIN_TENDER_VALUE = 1_000_000_000
MAX_PROCESSES = os.cpu_count()  # Use all CPUs
BATCH_SIZE = 512

def process_single_tender(tender_id):
    report = {"tender_id": tender_id, "processed_docs": 0, "skipped_docs": 0, "errors": []}
    try:
        print(f"[{tender_id}] 🚀 Starting tender")
        s3_prefix = f"tender-documents/{tender_id}/"
        pdf_keys = list_pdfs(s3_prefix)   
        print(f"[{tender_id}] Found {len(pdf_keys)} PDFs")

        for key in pdf_keys:
            document_name = key.split("/")[-1]

            if document_exists_for_tender(tender_id, document_name):
                report["skipped_docs"] += 1
                continue

            try:
                pdf_stream = download_pdf(key)
                sub_chunks = process_pdf_to_subchunks(pdf_stream, document_name)
                if not sub_chunks:
                    report["skipped_docs"] += 1
                    continue

                texts = [sc["data"] for sc in sub_chunks]
                embeddings = embed_texts(texts, batch_size=BATCH_SIZE)

                docs = [
                    {
                        "tender_id": tender_id,
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

                insert_vectors(docs)
                report["processed_docs"] += 1

            except Exception as e_doc:
                report["errors"].append(f"Doc {document_name}: {e_doc}")

        print(f"[{tender_id}] 🏁 Finished tender - Processed: {report['processed_docs']}, Skipped: {report['skipped_docs']}, Errors: {len(report['errors'])}")

    except Exception as e:
        report["errors"].append(str(e))
        print(f"[{tender_id}] ❌ Tender-level error: {e}")

    return report

# ================= MAIN FUNCTION =================
def main():
    print("🔍 Fetching tender IDs from MongoDB...")
    tender_ids = get_tender_ids(MIN_TENDER_VALUE)
    print(f"📦 Found {len(tender_ids)} tenders above {MIN_TENDER_VALUE}\n")

    print(f"🧠 Using {MAX_PROCESSES} parallel processes\n")

    reports = []
    with ProcessPoolExecutor(max_workers=MAX_PROCESSES) as executor:
        # Submit all tasks
        futures = {executor.submit(process_single_tender, tid): tid for tid in tender_ids}

        # Use tqdm to track progress
        for f in tqdm(as_completed(futures), total=len(futures), desc="Processing tenders"):
            report = f.result()
            reports.append(report)

    # Summary
    total_docs = sum(r["processed_docs"] for r in reports)
    total_skipped = sum(r["skipped_docs"] for r in reports)
    total_errors = sum(len(r["errors"]) for r in reports)
    print(f"\n✅ All tenders processed! Total docs: {total_docs}, Skipped: {total_skipped}, Errors: {total_errors}")

if __name__ == "__main__":
    main()

