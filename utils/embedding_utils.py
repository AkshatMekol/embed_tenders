import os
import openai
from utils.config import BATCH_SIZE, OPENAI_API_KEY, EMBEDDING_MODEL

openai.api_key = OPENAI_API_KEY

def embed_batch(chunks):
    texts = [c["data"] for c in chunks]

    vectors = []
    for i in range(0, len(texts), BATCH_SIZE):
        batch_texts = texts[i:i+BATCH_SIZE]
        # NEW v1+ API
        response = openai.embeddings.create(
            model=EMBEDDING_MODEL,
            input=batch_texts
        )
        batch_vectors = [item.embedding for item in response.data]
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
