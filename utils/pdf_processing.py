# import gc
# import pdfplumber
# from io import BytesIO
# import asyncio
# from utils.chunking import split_text_to_subchunks
# from utils.config import MAX_PROCESSES_DEEPSEEK, MAX_PROCESSES_GROQ, PDF_BATCH_SIZE
# from utils.regular_helpers import extract_page_content, elements_to_positions
# from utils.scanned_helpers import is_scanned_page, process_scanned_page_worker, deepseek_translate_worker

# async def groq_worker(job, semaphore):
#     async with semaphore:
#         loop = asyncio.get_running_loop()
#         return await loop.run_in_executor(None, process_scanned_page_worker, job)
        
# async def deepseek_worker(job, semaphore):
#     async with semaphore:
#         loop = asyncio.get_running_loop()
#         res = await loop.run_in_executor(None, deepseek_translate_worker, job)
#         sub_chunks = split_text_to_subchunks(
#             res["translated_text"], res["page"], 1, "text", is_scanned=True
#         )
#         gc.collect()
#         return sub_chunks

# async def process_pdf_batch(pdf_bytes, start_page=0, end_page=None):
#     all_sub_chunks = []
#     scanned_jobs = []

#     with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
#         total_pages = len(pdf.pages)
#         if end_page is None or end_page > total_pages:
#             end_page = total_pages

#         for i in range(start_page, end_page):
#             page = pdf.pages[i]
#             scanned = is_scanned_page(page)
#             if scanned:
#                 scanned_jobs.append((i, pdf_bytes))
#             else:
#                 elements = extract_page_content(page)
#                 positions = elements_to_positions(elements)
#                 for pos in positions:
#                     sub_chunks = split_text_to_subchunks(
#                         pos["content"], i+1, pos["position"], pos["type"], is_scanned=False
#                     )
#                     all_sub_chunks.extend(sub_chunks)
#         gc.collect()

#     groq_results = []
#     if scanned_jobs:
#         groq_semaphore = asyncio.Semaphore(MAX_PROCESSES_GROQ)
#         groq_tasks = [groq_worker(job, groq_semaphore) for job in scanned_jobs]
#         groq_results = await asyncio.gather(*groq_tasks)

#     deepseek_results = []
#     if groq_results:
#         deepseek_jobs = [(res["page"], res["raw_content"]) for res in groq_results]
#         deepseek_semaphore = asyncio.Semaphore(MAX_PROCESSES_DEEPSEEK)
#         deepseek_tasks = [deepseek_worker(job, deepseek_semaphore) for job in deepseek_jobs]
#         deepseek_results = await asyncio.gather(*deepseek_tasks)
#         for sub_chunks in deepseek_results:
#             all_sub_chunks.extend(sub_chunks)
#         gc.collect()

#     gc.collect()
#     return all_sub_chunks, len(scanned_jobs), (end_page - start_page - len(scanned_jobs))

import gc
import pdfplumber
from io import BytesIO
import asyncio
from utils.chunking import split_text_to_subchunks
from utils.config import MAX_PROCESSES_DEEPSEEK, MAX_PROCESSES_GROQ, PDF_BATCH_SIZE
from utils.regular_helpers import extract_page_content, elements_to_positions
from utils.scanned_helpers import is_scanned_page, process_scanned_page_worker, deepseek_translate_worker


async def groq_worker(job, semaphore):
    async with semaphore:
        loop = asyncio.get_running_loop()
        try:
            return await loop.run_in_executor(
                None,
                process_scanned_page_worker,
                job
            )
        except Exception as e:
            print(f"[ERROR][GROQ WORKER] job={job} crashed → {e}")
            return {"page": job[0] + 1, "raw_content": f"<!-- groq_worker exception: {e} -->"}


async def deepseek_worker(job, semaphore):
    async with semaphore:
        loop = asyncio.get_running_loop()
        try:
            res = await loop.run_in_executor(
                None,
                deepseek_translate_worker,
                job
            )
        except Exception as e:
            print(f"[ERROR][DEEPSEEK WORKER] job={job} crashed → {e}")
            return []

        try:
            sub_chunks = split_text_to_subchunks(
                res["translated_text"], res["page"], 1, "text", is_scanned=True
            )
            return sub_chunks
        except Exception as e:
            print(f"[ERROR][DEEPSEEK CHUNKING] page={res['page']} → {e}")
            return []
        finally:
            gc.collect()


async def process_pdf_batch(pdf_bytes, start_page=0, end_page=None):
    all_sub_chunks = []
    scanned_jobs = []
    debug_failures = []  # collect stage failures

    print(f"[DEBUG] Opening PDF for pages {start_page} → {end_page}")

    try:
        pdf = pdfplumber.open(BytesIO(pdf_bytes))
    except Exception as e:
        print(f"[CRITICAL] PDF failed to open → {e}")
        return [], 0, 0

    with pdf:
        total_pages = len(pdf.pages)
        if end_page is None or end_page > total_pages:
            end_page = total_pages

        for i in range(start_page, end_page):
            page_num = i + 1
            print(f"[DEBUG][PAGE {page_num}] Entering page loop...")

            try:
                page = pdf.pages[i]
            except Exception as e:
                print(f"[ERROR][PAGE {page_num}] Failed loading page → {e}")
                debug_failures.append(("load_page", page_num, str(e)))
                continue

            # Determine scanned/non-scanned
            try:
                scanned = is_scanned_page(page)
                print(f"[DEBUG][PAGE {page_num}] scanned={scanned}")
            except Exception as e:
                print(f"[ERROR][PAGE {page_num}] is_scanned_page FAILED → {e}")
                debug_failures.append(("is_scanned_page", page_num, str(e)))
                continue

            # SCANNED PAGE
            if scanned:
                scanned_jobs.append((i, pdf_bytes))
                continue

            # NON-SCANNED PAGE
            try:
                elements = extract_page_content(page)
                print(f"[DEBUG][PAGE {page_num}] extracted elements={len(elements)}")
            except Exception as e:
                print(f"[ERROR][PAGE {page_num}] extract_page_content FAILED → {e}")
                debug_failures.append(("extract_page_content", page_num, str(e)))
                continue

            try:
                positions = elements_to_positions(elements)
                print(f"[DEBUG][PAGE {page_num}] positions={len(positions)}")
            except Exception as e:
                print(f"[ERROR][PAGE {page_num}] elements_to_positions FAILED → {e}")
                debug_failures.append(("elements_to_positions", page_num, str(e)))
                continue

            # Chunkify text
            for pos in positions:
                try:
                    sub_chunks = split_text_to_subchunks(
                        pos["content"],
                        page_num,
                        pos["position"],
                        pos["type"],
                        is_scanned=False
                    )
                    all_sub_chunks.extend(sub_chunks)
                except Exception as e:
                    print(f"[ERROR][PAGE {page_num}] split_text_to_subchunks FAILED → {e}")
                    debug_failures.append(("split_text_to_subchunks", page_num, str(e)))

        gc.collect()

    # GROQ
    groq_results = []
    if scanned_jobs:
        print(f"[DEBUG] Starting GROQ jobs: {len(scanned_jobs)}")
        groq_semaphore = asyncio.Semaphore(MAX_PROCESSES_GROQ)
        try:
            groq_results = await asyncio.gather(
                *[groq_worker(job, groq_semaphore) for job in scanned_jobs]
            )
        except Exception as e:
            print(f"[CRITICAL] groq_results gather() FAILED → {e}")
            debug_failures.append(("groq_gather", -1, str(e)))

    # DEEPSEEK
    if groq_results:
        deepseek_jobs = [(res["page"], res["raw_content"]) for res in groq_results]
        print(f"[DEBUG] Starting DeepSeek jobs: {len(deepseek_jobs)}")

        deepseek_semaphore = asyncio.Semaphore(MAX_PROCESSES_DEEPSEEK)
        try:
            deepseek_results = await asyncio.gather(
                *[deepseek_worker(job, deepseek_semaphore) for job in deepseek_jobs]
            )
        except Exception as e:
            print(f"[CRITICAL] deepseek gather() FAILED → {e}")
            debug_failures.append(("deepseek_gather", -1, str(e)))
            deepseek_results = []

        for sub_chunks in deepseek_results:
            all_sub_chunks.extend(sub_chunks)

    gc.collect()

    # PRINT ANY FAILURES
    if debug_failures:
        print("\n========= DEBUG FAILURES TRACE =========")
        for stage, page, err in debug_failures:
            print(f"[FAIL] stage={stage} | page={page} | error={err}")
        print("========================================\n")

    return all_sub_chunks, len(scanned_jobs), (end_page - start_page - len(scanned_jobs))
