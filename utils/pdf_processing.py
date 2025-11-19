import gc
import pdfplumber
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
from utils.chunking import split_text_to_subchunks
from utils.config import MAX_PROCESSES_DEEPSEEK, MAX_PROCESSES_GROQ
from utils.regular_helpers import extract_page_content, elements_to_positions
from utils.scanned_helpers import is_scanned_page, process_scanned_page_worker, deepseek_translate_worker

def process_pdf(pdf_stream):
    pdf_bytes = pdf_stream.read()
    all_sub_chunks = []
    scanned_jobs = []
    total_pages = 0

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        total_pages = len(pdf.pages)
        print(f"Total pages: {total_pages}")

        for i, page in enumerate(pdf.pages):
            scanned = is_scanned_page(page)
            print(f"\nProcessing Page {i+1}: {'SCANNED' if scanned else 'TEXT'}")

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

    scanned_pages_count = len(scanned_jobs)
    regular_pages_count = total_pages - scanned_pages_count

    groq_results = []
    if scanned_jobs:
        print(f"\nProcessing {scanned_pages_count} scanned pages with Groq in batches of {MAX_PROCESSES_GROQ}")

        for i in range(0, len(scanned_jobs), MAX_PROCESSES_GROQ):
            batch = scanned_jobs[i:i + MAX_PROCESSES_GROQ]
            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                futures = {executor.submit(process_scanned_page_worker, job): job for job in batch}
                for f in as_completed(futures):
                    res = f.result()
                    groq_results.append(res)

    deepseek_results = []
    if groq_results:
        deepseek_jobs = [(res["page"], res["raw_content"]) for res in groq_results]
        print(f"\nTranslating {len(deepseek_jobs)} pages with DeepSeek using {MAX_PROCESSES_DEEPSEEK} processes")

        with ThreadPoolExecutor(max_workers=MAX_PROCESSES_DEEPSEEK) as pool:
            for res in pool.map(deepseek_translate_worker, deepseek_jobs):
                sub_chunks = split_text_to_subchunks(
                    res["translated_text"], res["page"], 1, "text", is_scanned=True
                )
                all_sub_chunks.extend(sub_chunks)
                gc.collect()

    del pdf_bytes, scanned_jobs
    if 'groq_results' in locals():
        del groq_results
    if 'deepseek_results' in locals():
        del deepseek_results
    gc.collect()

    return {
        "chunks": all_sub_chunks,
        "scanned_pages": scanned_pages_count,
        "regular_pages": regular_pages_count
    }
