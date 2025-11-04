from sentence_transformers import SentenceTransformer

model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2", device="cpu")

def embed_texts(texts, batch_size=512):
    return model.encode(texts, batch_size=batch_size, show_progress_bar=False).tolist()
