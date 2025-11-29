import gc
import asyncio
import pdfplumber
from io import BytesIO
from utils.chunking import split_text_to_subchunks
from utils.regular_helpers import extract_page_content, elements_to_positions
from utils.config import MAX_PROCESSES_DEEPSEEK, MAX_PROCESSES_GROQ, PDF_BATCH_SIZE
from utils.scanned_helpers import is_scanned_page, process_scanned_page_worker, deepseek_translate_worker

async def groq_worker(job, semaphore):
    async with semaphore:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, process_scanned_page_worker, job)
        
async def deepseek_worker(job, semaphore):
    async with semaphore:
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(None, deepseek_translate_worker, job)
        sub_chunks = split_text_to_subchunks(
            res["translated_text"], res["page"], 1, "text", is_scanned=True
        )
        gc.collect()
        return sub_chunks

async def process_pdf_batch(pdf_bytes, start_page=0, end_page=None):
    all_sub_chunks = []
    scanned_jobs = []

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        total_pages = len(pdf.pages)
        if end_page is None or end_page > total_pages:
            end_page = total_pages

        for i in range(start_page, end_page):
            page = pdf.pages[i]
            scanned = is_scanned_page(page)
            if scanned:
                scanned_jobs.append((i, pdf_bytes))
            else:
                elements = extract_page_content(page)
                positions = elements_to_positions(elements)
                for pos in positions:
                    sub_chunks = split_text_to_subchunks(
                        pos["content"], i+1, pos["position"], pos["type"], is_scanned=False
                    )
                    all_sub_chunks.extend(sub_chunks)
        gc.collect()

    groq_results = []
    if scanned_jobs:
        groq_semaphore = asyncio.Semaphore(MAX_PROCESSES_GROQ)
        groq_tasks = [groq_worker(job, groq_semaphore) for job in scanned_jobs]
        groq_results = await asyncio.gather(*groq_tasks)

    deepseek_results = []
    if groq_results:
        deepseek_jobs = [(res["page"], res["raw_content"]) for res in groq_results]
        deepseek_semaphore = asyncio.Semaphore(MAX_PROCESSES_DEEPSEEK)
        deepseek_tasks = [deepseek_worker(job, deepseek_semaphore) for job in deepseek_jobs]
        deepseek_results = await asyncio.gather(*deepseek_tasks)
        for sub_chunks in deepseek_results:
            all_sub_chunks.extend(sub_chunks)
        gc.collect()

    gc.collect()
    return all_sub_chunks, len(scanned_jobs), (end_page - start_page - len(scanned_jobs))

# import gc
# import pdfplumber
# from io import BytesIO
# import asyncio
# import requests

# from utils.chunking import split_text_to_subchunks
# from utils.config import MAX_PROCESSES_DEEPSEEK, MAX_PROCESSES_GROQ
# from utils.regular_helpers import extract_page_content, elements_to_positions
# from utils.scanned_helpers import is_scanned_page, process_scanned_page_worker, deepseek_translate_worker

# GPU_SERVER_URL = "http://127.0.0.1:9000/enqueue"


# # ----------------- WORKERS -----------------
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


# # ----------------- SEND TO GPU -----------------
# async def send_to_gpu(chunks, page_num, doc_name="doc", tender_id="tender_id", is_last=False):
#     try:
#         resp = await asyncio.to_thread(
#             requests.post,
#             GPU_SERVER_URL,
#             json={
#                 "chunks": chunks,
#                 "document_name": f"{doc_name}_page_{page_num}",
#                 "tender_id": tender_id,
#                 "is_last_batch": is_last
#             }
#         )
#         print(f"[GPU] Sent page {page_num} → {resp.status_code}")
#         resp.close()
#     except Exception as e:
#         print(f"[GPU ERROR] page {page_num} → {e}")


# # ----------------- PROCESS PDF PAGES -----------------
# async def process_pdf_pages(pdf_bytes, doc_name="doc", tender_id="tender_id", start_page=0, end_page=None):
#     groq_semaphore = asyncio.Semaphore(MAX_PROCESSES_GROQ)
#     deepseek_semaphore = asyncio.Semaphore(MAX_PROCESSES_DEEPSEEK)
#     pending_tasks = []

#     with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
#         total_pages = len(pdf.pages)
#         if end_page is None or end_page > total_pages:
#             end_page = total_pages

#         for i in range(start_page, end_page):
#             page_num = i + 1
#             page = pdf.pages[i]
#             scanned = is_scanned_page(page)

#             # ----- SCANNED PAGE -----
#             if scanned:
#                 async def process_scanned(page_idx, page_bytes):
#                     groq_result = await groq_worker((page_idx, page_bytes), groq_semaphore)
#                     deepseek_result = await deepseek_worker(
#                         (groq_result["page"], groq_result["raw_content"]), deepseek_semaphore
#                     )
#                     if deepseek_result:
#                         is_last = (page_idx + 1 == total_pages)
#                         await send_to_gpu(deepseek_result, page_idx + 1, doc_name, tender_id, is_last=is_last)
                
#                 task = asyncio.create_task(process_scanned(i, pdf_bytes))
#                 pending_tasks.append(task)
#                 print(f"[SCANNED] Page {page_num} scheduled for GROQ+DEEPSEEK")
#                 continue

#             # ----- REGULAR PAGE -----
#             elements = extract_page_content(page)
#             positions = elements_to_positions(elements)
#             all_sub_chunks = []

#             for pos in positions:
#                 sub_chunks = split_text_to_subchunks(
#                     pos["content"], page_num, pos["position"], pos["type"], is_scanned=False
#                 )
#                 all_sub_chunks.extend(sub_chunks)

#             if all_sub_chunks:
#                 is_last = (page_num == total_pages)
#                 task = asyncio.create_task(send_to_gpu(all_sub_chunks, page_num, doc_name, tender_id, is_last=is_last))
#                 pending_tasks.append(task)

#             gc.collect()

#     # Wait for all GPU tasks to complete (both scanned & regular)
#     if pending_tasks:
#         await asyncio.gather(*pending_tasks)

#     print("✅ All pages processed and sent to GPU.")
