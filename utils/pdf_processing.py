import gc
import asyncio
import requests
import pdfplumber
from io import BytesIO
from utils.chunking import split_text_to_subchunks
from utils.config import MAX_PROCESSES_DEEPSEEK, MAX_PROCESSES_GROQ
from utils.regular_helpers import extract_page_content, elements_to_positions
from utils.scanned_helpers import is_scanned_page, process_scanned_page_worker, deepseek_translate_worker

EMBED_SERVER_URL = "http://127.0.0.1:9000/enqueue"

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

async def send_to_embed(chunks, page_num, doc_name="doc", tender_id="tender_id", is_last=False):
    try:
        resp = await asyncio.to_thread(
            requests.post,
            EMBED_SERVER_URL,
            json={
                "chunks": chunks,
                "document_name": f"{doc_name}_page_{page_num}",
                "tender_id": tender_id,
                "is_last_batch": is_last
            }
        )
        print(f"[EMBED] Sent page {page_num} → {resp.status_code}")
        resp.close()
    except Exception as e:
        print(f"[EMBED ERROR] page {page_num} → {e}")

async def process_pdf(pdf_bytes, doc_name="doc", tender_id="tender_id"):
    scanned_count = 0
    regular_count = 0
    total_chunks = 0

    groq_semaphore = asyncio.Semaphore(MAX_PROCESSES_GROQ)
    deepseek_semaphore = asyncio.Semaphore(MAX_PROCESSES_DEEPSEEK)
    pending_tasks = []

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        total_pages = len(pdf.pages)

        for i, page in enumerate(pdf.pages):
            page_num = i + 1
            scanned = is_scanned_page(page)

            if scanned:
                scanned_count += 1

                async def process_scanned(page_idx, page_bytes):
                    groq_result = await groq_worker((page_idx, page_bytes), groq_semaphore)
                    deepseek_result = await deepseek_worker(
                        (groq_result["page"], groq_result["raw_content"]), deepseek_semaphore
                    )
                    if deepseek_result:
                        await send_to_embed(deepseek_result, page_idx + 1, doc_name, tender_id,
                                            is_last=(page_idx + 1 == total_pages))
                    return len(deepseek_result) if deepseek_result else 0

                task = asyncio.create_task(process_scanned(i, pdf_bytes))
                pending_tasks.append(task)
                continue

            regular_count += 1
            elements = extract_page_content(page)
            positions = elements_to_positions(elements)
            all_sub_chunks = []

            for pos in positions:
                sub_chunks = split_text_to_subchunks(
                    pos["content"], page_num, pos["position"], pos["type"], is_scanned=False
                )
                all_sub_chunks.extend(sub_chunks)

            if all_sub_chunks:
                task = asyncio.create_task(send_to_embed(all_sub_chunks, page_num, doc_name, tender_id,
                                                         is_last=(page_num == total_pages)))
                pending_tasks.append(task)
                total_chunks += len(all_sub_chunks)

            gc.collect()

    if pending_tasks:
        results = await asyncio.gather(*pending_tasks)
        total_chunks += sum([r for r in results if isinstance(r, int)])

    return scanned_count, regular_count, total_chunks
