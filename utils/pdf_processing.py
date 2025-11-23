import gc
import asyncio
from typing import Tuple, List
from utils.chunking import split_text_to_subchunks
from utils.regular_helpers import extract_page_content, elements_to_positions
from utils.scanned_helpers import is_scanned_page, process_scanned_page_worker, deepseek_translate_worker
from utils.config import MAX_PROCESSES_GROQ, MAX_PROCESSES_DEEPSEEK

async def groq_worker(job, semaphore):
    async with semaphore:
        loop = asyncio.get_running_loop()
        # process_scanned_page_worker should accept the page object
        return await loop.run_in_executor(None, process_scanned_page_worker, job)

async def deepseek_worker(job, semaphore):
    async with semaphore:
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(None, deepseek_translate_worker, job)
        sub_chunks = split_text_to_subchunks(
            res["translated_text"], res["page"], 1, "text", is_scanned=True
        )
        # free intermediate memory ASAP
        gc.collect()
        return sub_chunks

async def process_pdf_batch(pdf, start_page: int = 0, end_page: int = None) -> Tuple[List[dict], int, int]:
    all_sub_chunks = []
    scanned_jobs = []

    total_pages = len(pdf.pages)
    if end_page is None or end_page > total_pages:
        end_page = total_pages

    # Process pages from pdf.pages WITHOUT reopening document
    for i in range(start_page, end_page):
        page = pdf.pages[i]
        scanned = is_scanned_page(page)
        if scanned:
            # append page object (or minimal representation) to scanned_jobs
            # process_scanned_page_worker should accept (page_number, page) or similar
            scanned_jobs.append((i, page))
        else:
            # regular (text) page extraction
            elements = extract_page_content(page)
            positions = elements_to_positions(elements)
            for pos in positions:
                sub_chunks = split_text_to_subchunks(
                    pos["content"], i+1, pos["position"], pos["type"], is_scanned=False
                )
                all_sub_chunks.extend(sub_chunks)

            del elements
            del positions
            gc.collect()

    scanned_count = len(scanned_jobs)
    groq_results = []
    if scanned_jobs:
        groq_semaphore = asyncio.Semaphore(MAX_PROCESSES_GROQ)
        groq_tasks = [groq_worker(job, groq_semaphore) for job in scanned_jobs]
        # Note: each groq_worker returns processed raw_content dict
        groq_results = await asyncio.gather(*groq_tasks)

    deepseek_results = []
    if groq_results:
        deepseek_jobs = [(res["page"], res["raw_content"]) for res in groq_results]
        deepseek_semaphore = asyncio.Semaphore(MAX_PROCESSES_DEEPSEEK)
        deepseek_tasks = [deepseek_worker(job, deepseek_semaphore) for job in deepseek_jobs]
        deepseek_results = await asyncio.gather(*deepseek_tasks)
        for sub_chunks in deepseek_results:
            all_sub_chunks.extend(sub_chunks)
            # free sub_chunks reference after extending
            del sub_chunks
            gc.collect()

    regular_pages_count = (end_page - start_page) - scanned_count
    gc.collect()
    return all_sub_chunks, scanned_count, regular_pages_count


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

#     gc.collect()
#     return all_sub_chunks, len(scanned_jobs), (end_page - start_page - len(scanned_jobs))
