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


async def send_to_embed(chunks_batch, tender_id, is_last=False):
    try:
        resp = await asyncio.to_thread(
            requests.post,
            EMBED_SERVER_URL,
            json={
                "chunks": chunks_batch,
                "tender_id": tender_id,
                "is_last_batch": is_last
            }
        )
        print(f"[EMBED] Sent batch → {resp.status_code} (items={len(chunks_batch)})")
        resp.close()
    except Exception as e:
        print(f"[EMBED ERROR] batch → {e}")

async def process_pdf(pdf_bytes, doc_name="doc", tender_id="tender_id"):
    scanned_count = 0
    regular_count = 0
    total_chunks = 0

    groq_semaphore = asyncio.Semaphore(MAX_PROCESSES_GROQ)
    deepseek_semaphore = asyncio.Semaphore(MAX_PROCESSES_DEEPSEEK)
    batch_buffer = []  
    
    file_size_kb = len(pdf_bytes) / 1024
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        total_pages = len(pdf.pages)

    size_per_page_kb = file_size_kb / max(total_pages, 1)
    batch_size = 20 if size_per_page_kb < 250 else 5

    print(f"📦 Dynamic batch size = {batch_size} (avg {size_per_page_kb:.1f} KB/page)")

    async def flush_batch(is_last=False):
        nonlocal batch_buffer
        if not batch_buffer:
            return
        await send_to_embed(batch_buffer, tender_id, is_last=is_last)
        batch_buffer = []

    pending_tasks = []

    async with asyncio.TaskGroup() as tg:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for i, page in enumerate(pdf.pages):
                page_num = i + 1
                scanned = is_scanned_page(page)

                if scanned:
                    scanned_count += 1

                    async def handle_scanned(idx):
                        groq_result = await groq_worker((idx, pdf_bytes), groq_semaphore)
                        deep_chunks = await deepseek_worker(
                            (groq_result["page"], groq_result["raw_content"]),
                            deepseek_semaphore
                        )
                        return deep_chunks or []

                    t = tg.create_task(handle_scanned(i))
                    pending_tasks.append((page_num, t))
                    continue

                regular_count += 1
                elements = extract_page_content(page)
                positions = elements_to_positions(elements)

                page_chunks = []
                for pos in positions:
                    page_chunks.extend(
                        split_text_to_subchunks(
                            pos["content"], page_num, pos["position"], pos["type"], is_scanned=False
                        )
                    )

                if page_chunks:
                    for ch in page_chunks:
                        batch_buffer.append(ch)
                        if len(batch_buffer) >= batch_size:
                            await flush_batch(is_last=False)

                gc.collect()

        for page_num, task in pending_tasks:
            result_chunks = await task
            total_chunks += len(result_chunks)

            for ch in result_chunks:
                batch_buffer.append(ch)
                if len(batch_buffer) >= batch_size:
                    await flush_batch(is_last=False)

    await flush_batch(is_last=True)

    return scanned_count, regular_count, total_chunks
