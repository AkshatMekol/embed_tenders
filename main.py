import asyncio
import aiohttp
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
from utils.mongo_utils import get_tender_ids  

SERVER_URL = "http://13.203.30.125:8000/process/"
MAX_CONCURRENT = 1
MIN_VALUE = 2000000000

async def process_tender(session, tender_id):
    url = SERVER_URL + tender_id
    try:
        async with session.post(url) as resp:
            if resp.status != 200:
                return {"tender_id": tender_id, "error": f"HTTP {resp.status}"}
            return await resp.json()
    except Exception as e:
        return {"tender_id": tender_id, "error": str(e)}

async def runner(tender_ids):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    results = []

    async with aiohttp.ClientSession() as session:
        async def sem_task(tid):
            async with semaphore:
                return await process_tender(session, tid)

        tasks = [sem_task(tid) for tid in tender_ids]

        # THIS is the correct tqdm usage
        results = await tqdm_asyncio.gather(
            *tasks,
            total=len(tasks),
            desc="Processing tenders"
        )

        # Print each result as they complete
        for result in results:
            print("\n===============================")
            print(f"📦 Tender {result.get('tender_id')} finished")
            print("===============================")

            if "error" in result:
                print(f" ❌ Error: {result['error']}")
            else:
                print(f" ✔ Docs processed: {result.get('processed_docs')}")
                print(f" ↪ Skipped: {result.get('skipped_docs')}")
                print(f" ↪ Empty: {result.get('empty_docs')}")
                print(f" ↪ Scanned pages: {result.get('scanned_pages')}")
                print(f" ↪ Regular pages: {result.get('regular_pages')}")
                print(f" ↪ Errors: {len(result.get('errors', []))}")
            print("===============================\n")

    return results


def main():
    print("Fetching tender IDs...")
    tender_ids = get_tender_ids(MIN_VALUE)   

    print(f"Found {len(tender_ids)} tenders.")
    print("Sending them to server (max 4 at a time)...")

    results = asyncio.run(runner(tender_ids))

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
