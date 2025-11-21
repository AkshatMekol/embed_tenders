"""Simple FastAPI server for GPU Embedding with internal queue."""

import gc
import asyncio
import torch
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
from utils.config import EMBEDDING_MODEL_NAME, BATCH_SIZE
from utils.mongo_utils import store_embeddings_in_db
from contextlib import asynccontextmanager

# Global queue and model
task_queue = asyncio.Queue(maxsize=1000)
model = None
processing_task = None


class EmbeddingTask(BaseModel):
    chunks: list
    document_name: str
    tender_id: str


async def embed_batch(chunks):
    """Embed a batch of chunks using the model."""
    texts = [c["data"] for c in chunks]
    # Run blocking encode in executor
    loop = asyncio.get_event_loop()
    vectors = await loop.run_in_executor(
        None,
        lambda: model.encode(
            texts, batch_size=BATCH_SIZE, show_progress_bar=False
        ).tolist(),
    )

    out = []
    for c, emb in zip(chunks, vectors):
        out.append(
            {
                "tender_id": c["tender_id"],
                "document_name": c["document_name"],
                "page": c["page"],
                "position": c["position"],
                "sub_position": c["sub_position"],
                "type": c["type"],
                "is_scanned": c["is_scanned"],
                "text": c["data"],
                "embedding": emb,
            }
        )

    return out


async def process_task(task: EmbeddingTask):
    """Process a single task: embed chunks and store in MongoDB."""
    chunks, document_name, tender_id = task.chunks, task.document_name, task.tender_id

    # Ensure chunks have required fields
    for c in chunks:
        if "tender_id" not in c:
            c["tender_id"] = tender_id
        if "document_name" not in c:
            c["document_name"] = document_name

    try:
        embeddings = await embed_batch(chunks)
        print(f"[{document_name}] ✅ Successfully vectorized {len(chunks)} chunks")

        # Run blocking MongoDB insert in executor
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: store_embeddings_in_db(embeddings, document_name, tender_id),
        )
        print(f"[{document_name}] 💾 Successfully stored embeddings in MongoDB")

        gc.collect()
        return {
            "status": "success",
            "document_name": document_name,
            "chunks_count": len(chunks),
        }
    except Exception as e:
        print(f"[Embedding Server] ❌ Error embedding {document_name}: {e}")
        return {"status": "error", "document_name": document_name, "error": str(e)}


async def worker_loop():
    """Background worker that processes tasks from the queue."""
    print("🚀 Embedding worker started processing from queue")
    while True:
        try:
            # Get task from queue (blocks until available)
            task = await task_queue.get()

            print(f"📥 Processing task: {task.document_name}")
            result = await process_task(task)
            print(f"✅ Task completed: {result}")

            # Mark task as done
            task_queue.task_done()
        except Exception as e:
            print(f"[Embedding Worker] ❌ Error processing task: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler - initialize model and start worker."""
    global model, processing_task

    # Startup
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"🚀 Initializing embedding model on device: {device}")
    model = SentenceTransformer(EMBEDDING_MODEL_NAME, device=device)
    print(f"✅ Embedding model loaded on {device}")

    # Start background worker
    processing_task = asyncio.create_task(worker_loop())

    yield

    # Shutdown
    print("🛑 Shutting down embedding server...")
    if processing_task:
        processing_task.cancel()
        try:
            await processing_task
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)


@app.post("/enqueue")
async def enqueue_task(task: EmbeddingTask):
    """
    Enqueue an embedding task.

    Returns immediately with success status.
    Task will be processed asynchronously by the worker.
    """
    try:
        # Put task in queue (non-blocking if queue has space)
        await task_queue.put(task)
        return {
            "status": "success",
            "message": "Task enqueued",
            "document_name": task.document_name,
            "tender_id": task.tender_id,
            "chunks_count": len(task.chunks),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to enqueue task: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "queue_size": task_queue.qsize(),
        "queue_maxsize": task_queue.maxsize,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=9000)
