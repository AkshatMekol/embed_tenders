import os
import gc
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from utils.s3_utils import list_s3_pdfs, fetch_pdf
from utils.pdf_processing import process_pdf
from utils.mongo_utils import get_tender_ids, vector_collection, enqueue_chunks_for_embedding, embedding_thread, embedding_queue

MIN_TENDER_VALUE = 1_000_000_000
MAX_PROCESSES = 1  
BATCH_SIZE = 512

def process_single_tender(tender_id):
    report = {
        "tender_id": tender_id,
        "processed_docs": 0,
        "skipped_docs": 0,
        "empty_docs": 0,
        "scanned_pages": 0,
        "regular_pages": 0,
        "errors": []
    }

    try:
        print(f"[{tender_id}] Starting tender")
        s3_prefix = f"tender-documents/{tender_id}/"
        pdf_keys = list_s3_pdfs(s3_prefix)
        print(f"[{tender_id}] Found {len(pdf_keys)} PDFs")

        if not pdf_keys:
            print(f"[{tender_id}] No PDFs found")
            return report

        for pdf_key in pdf_keys:
            document_name = os.path.basename(pdf_key)

            if vector_collection.count_documents({"tender_id": tender_id, "document_name": document_name}) > 0:
                report["skipped_docs"] += 1
                print(f"[{tender_id}] Skipping existing PDF: {document_name}")
                continue

            try:
                pdf_stream = fetch_pdf(pdf_key)
                pdf_result = process_pdf(pdf_stream)

                sub_chunks = pdf_result["chunks"]
                scanned_pages = pdf_result["scanned_pages"]
                regular_pages = pdf_result["regular_pages"]

                report["scanned_pages"] += scanned_pages
                report["regular_pages"] += regular_pages

                if not sub_chunks:
                    report["empty_docs"] += 1
                    print(f"[{tender_id}] PDF {document_name} has no subchunks")
                    continue

                enqueue_chunks_for_embedding(sub_chunks, document_name, tender_id)
                report["processed_docs"] += 1

                print(f"[{tender_id}] PDF {document_name} processed: Regular={regular_pages}, Scanned={scanned_pages}")

                del pdf_stream, sub_chunks
                gc.collect()

            except Exception as e_pdf:
                report["errors"].append(f"{document_name}: {e_pdf}")
                print(f"[{tender_id}] ❌ Error processing PDF {document_name}: {e_pdf}")

        print(f"[{tender_id}] Finished tender - Processed: {report['processed_docs']}, Skipped: {report['skipped_docs']}, Empty: {report['empty_docs']}, Scanned Pages: {report['scanned_pages']}, Regular Pages: {report['regular_pages']}, Errors: {len(report['errors'])}")

    except Exception as e:
        report["errors"].append(str(e))
        print(f"[{tender_id}] ❌ Tender-level error: {e}")

    return report

def main():
    print("Fetching tender IDs from MongoDB...")
    # tender_ids = get_tender_ids(MIN_TENDER_VALUE)
    tender_ids = ["6910f5f5b29e12b878b4f666"]
    print(f"Found {len(tender_ids)} tenders above {MIN_TENDER_VALUE}\n")
    print(f"Using {MAX_PROCESSES} parallel CPU processes\n")

    reports = []

    try:
        with ProcessPoolExecutor(max_workers=MAX_PROCESSES) as executor:
            futures = {executor.submit(process_single_tender, tid): tid for tid in tender_ids}

            try:
                for f in tqdm(as_completed(futures), total=len(futures), desc="Processing tenders"):
                    report = f.result()
                    reports.append(report)

            except KeyboardInterrupt:
                print("\n⚠️ Ctrl+C detected! Shutting down executor...")
                executor.shutdown(wait=False, cancel_futures=True)
                for f in futures:
                    f.cancel()
                raise

    except KeyboardInterrupt:
        print("Stopped by user.")

    print("Waiting for embedding queue to finish...")
    embedding_queue.join()
    embedding_queue.put(None)
    embedding_thread.join()
    print("Embedding thread has finished all work")

    total_docs = sum(r["processed_docs"] for r in reports)
    total_skipped = sum(r["skipped_docs"] for r in reports)
    total_empty = sum(r["empty_docs"] for r in reports)
    total_scanned_pages = sum(r["scanned_pages"] for r in reports)
    total_regular_pages = sum(r["regular_pages"] for r in reports)
    total_errors = sum(len(r["errors"]) for r in reports)

    print(f"\nTenders processed before stop:")
    print(f"Total docs processed: {total_docs}")
    print(f"Skipped (already in DB): {total_skipped}")
    print(f"Empty PDFs (no subchunks): {total_empty}")
    print(f"Scanned pages total: {total_scanned_pages}")
    print(f"Regular pages total: {total_regular_pages}")
    print(f"Errors: {total_errors}")

if __name__ == "__main__":
    main()
