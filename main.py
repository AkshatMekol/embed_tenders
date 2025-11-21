import requests
from utils.mongo_utils import get_tender_ids

SERVER_URL = "http://13.203.30.125:8000/process/"
MIN_VALUE = 2000000000


def process_tender(tender_id):
    url = SERVER_URL + tender_id
    try:
        resp = requests.post(url, timeout=None)
        if resp.status_code != 200:
            return {"tender_id": tender_id, "error": f"HTTP {resp.status_code}"}
        return resp.json()
    except Exception as e:
        return {"tender_id": tender_id, "error": str(e)}


def main():
    print("Fetching tender IDs...")
    tender_ids = get_tender_ids(MIN_VALUE)

    total = len(tender_ids)
    print(f"Found {total} tenders.")
    print("Processing them one by one...\n")

    results = []

    for idx, tender_id in enumerate(tender_ids, start=1):
        print(f"▶ Processing tender {tender_id} ({idx}/{total})")
        result = process_tender(tender_id)
        results.append(result)

        print(f"done {idx}/{total}\n")

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
