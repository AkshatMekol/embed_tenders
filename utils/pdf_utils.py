import pdfplumber

def extract_page_content(page):
    words = page.extract_words()
    lines = {}
    for word in words:
        top = round(word["top"])
        lines.setdefault(top, []).append(word["text"])
    elements = [{"type": "text", "top": k, "content": " ".join(v)} for k, v in lines.items()]
    elements.sort(key=lambda e: e["top"])
    return elements

def split_position_to_subchunks(position, page_num, chunk_size=150, overlap=25):
    text = position["content"]
    sub_chunks = []
    start = 0
    sub_pos = 1
    while start < len(text):
        end = start + chunk_size
        sub_text = text[start:end].strip()
        sub_chunks.append({
            "page": page_num,
            "position": 1,
            "sub_position": sub_pos,
            "type": position["type"],
            "data": sub_text,
            "document_name": ""
        })
        sub_pos += 1
        start = end - overlap
    return sub_chunks

def process_pdf_to_subchunks(pdf_stream, document_name):
    all_sub_chunks = []
    with pdfplumber.open(pdf_stream) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            elements = extract_page_content(page)
            for el in elements:
                scs = split_position_to_subchunks(el, page_num)
                for sc in scs:
                    sc["document_name"] = document_name
                all_sub_chunks.extend(scs)
    return all_sub_chunks
