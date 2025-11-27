# from sentence_transformers import SentenceTransformer
# from utils.config import EMBEDDING_MODEL_NAME, device, BATCH_SIZE

# model = SentenceTransformer(EMBEDDING_MODEL_NAME, device=device)

# def embed_batch(chunks):
#     texts = [c["data"] for c in chunks]
#     vectors = model.encode(texts, batch_size=BATCH_SIZE, show_progress_bar=False).tolist()

#     out = []
#     for c, emb in zip(chunks, vectors):
#         out.append({
#             "tender_id": c["tender_id"],
#             "document_name": c["document_name"],
#             "page": c["page"],
#             "position": c["position"],
#             "sub_position": c["sub_position"],
#             "type": c["type"],
#             "is_scanned": c["is_scanned"],
#             "text": c["data"],
#             "embedding": emb
#         })

#     return out

import os
import openai
from utils.config import BATCH_SIZE, OPENAI_API_KEY, EMBEDDING_MODEL

openai.api_key = OPENAI_API_KEY

def embed_batch(chunks):
    texts = [c["data"] for c in chunks]

    # OpenAI embeddings support batch requests
    # Split into BATCH_SIZE to avoid large requests
    vectors = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch_texts = texts[i:i+BATCH_SIZE]
        response = openai.Embedding.create(
            input=batch_texts,
            model=EMBEDDING_MODEL
        )
        batch_vectors = [item["embedding"] for item in response["data"]]
        vectors.extend(batch_vectors)

    out = []
    for c, emb in zip(chunks, vectors):
        out.append({
            "tender_id": c["tender_id"],
            "document_name": c["document_name"],
            "page": c["page"],
            "position": c["position"],
            "sub_position": c["sub_position"],
            "type": c["type"],
            "is_scanned": c["is_scanned"],
            "text": c["data"],
            "embedding": emb
        })

    return out
