import requests
from utils.mongo_utils import get_tender_ids
from concurrent.futures import ThreadPoolExecutor, as_completed

SERVER_URL = "http://127.0.0.1:8000/process/"
MIN_VALUE = 2000000000
MAX_WORKERS = 4  

def process_tender(tender_id):
    url = SERVER_URL + tender_id
    print(f"▶ Starting tender {tender_id}")

    try:
        resp = requests.post(url, timeout=None)
        if resp.status_code != 200:
            print(f"❌ Tender {tender_id} failed (HTTP {resp.status_code})")
            return {"tender_id": tender_id, "error": f"HTTP {resp.status_code}"}

        print(f"✔ Finished tender {tender_id}")
        return resp.json()

    except Exception as e:
        print(f"❌ Tender {tender_id} error: {e}")
        return {"tender_id": tender_id, "error": str(e)}

def main():
    print("Fetching tender IDs...")
    tender_ids = get_tender_ids(MIN_VALUE)

    total = len(tender_ids)
    print(f"Found {total} tenders.\n")
    print(f"Processing with {MAX_WORKERS} parallel workers...\n")

    results = []
    completed_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_tender = {executor.submit(process_tender, t): t for t in tender_ids}

        for future in as_completed(future_to_tender):
            tender_id = future_to_tender[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {"tender_id": tender_id, "error": str(exc)}

            results.append(result)
            completed_count += 1
            print(f"Progress: {completed_count}/{total} done\n")

    print("\n==================== FINAL SUMMARY ====================")

    total_docs = sum(r.get("processed_docs", 0) for r in results)
    total_skipped = sum(r.get("skipped_docs", 0) for r in results)
    total_empty = sum(r.get("empty_docs", 0) for r in results)
    total_scanned = sum(r.get("scanned_pages", 0) for r in results)
    total_regular = sum(r.get("regular_pages", 0) for r in results)
    total_errors = sum(len(r.get("errors", [])) for r in results if "errors" in r)

    print(f"Total docs processed: {total_docs}")
    print(f"Total skipped: {total_skipped}")
    print(f"Total empty PDFs: {total_empty}")
    print(f"Total scanned pages: {total_scanned}")
    print(f"Total regular pages: {total_regular}")
    print(f"Errors: {total_errors}")

    print("========================================================")

if __name__ == "__main__":
    main()
