from multiprocessing import Pool

def process_pdf(pdf_stream):
    pdf_bytes = pdf_stream.read()

    all_sub_chunks = []
    scanned_jobs = []
    total_pages = 0

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        total_pages = len(pdf.pages)
        print(f"📄 Total pages: {total_pages}")

        for i, page in enumerate(pdf.pages):
            scanned = is_scanned_page(page)
            print(f"\n📄 Processing Page {i+1}: {'SCANNED' if scanned else 'TEXT'}")

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

    if scanned_jobs:
        groq_results = []
        for args in scanned_jobs:
            result = process_scanned_page_worker(args)
            groq_results.append(result)

        deepseek_jobs = [(res["page"], res["raw_content"]) for res in groq_results]

        with Pool(MAX_PROCESSES_DEEPSEEK) as pool:
            deepseek_results = pool.map(deepseek_translate_worker, deepseek_jobs)

        for res in deepseek_results:
            sub_chunks = split_text_to_subchunks(
                res["translated_text"], res["page"], 1, "text", is_scanned=True
            )
            all_sub_chunks.extend(sub_chunks)
            gc.collect()
            print_memory_usage(f"after processing translated page {res['page']}")

    return {
        "chunks": all_sub_chunks,
        "scanned_pages": scanned_pages_count,
        "regular_pages": regular_pages_count
    }
