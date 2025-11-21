"""Simple client library for GPU Embedding service."""

import requests
from typing import List


class EmbeddingClient:
    """Simple client to enqueue embedding tasks."""

    def __init__(self, embedding_server_url: str = "http://localhost:9000"):
        """
        Initialize the embedding client.

        Args:
            embedding_server_url: URL of the embedding server (default: http://localhost:9000)
        """
        self.server_url = embedding_server_url.rstrip("/")
        self.enqueue_url = f"{self.server_url}/enqueue"

    def enqueue_task(
        self, chunks: List[dict], document_name: str, tender_id: str
    ) -> dict:
        """
        Enqueue an embedding task.

        Args:
            chunks: List of text chunks to embed
            document_name: Name of the document
            tender_id: ID of the tender

        Returns:
            dict: Response with status and task info

        Raises:
            requests.RequestException: If the request fails
        """
        task_data = {
            "chunks": chunks,
            "document_name": document_name,
            "tender_id": tender_id,
        }

        try:
            response = requests.post(
                self.enqueue_url,
                json=task_data,
                headers={"Content-Type": "application/json"},
                timeout=30,  # 30 second timeout for enqueueing
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise RuntimeError(
                f"Failed to enqueue task to embedding server: {e}"
            ) from e

    def health_check(self) -> dict:
        """
        Check if the embedding server is healthy.

        Returns:
            dict: Health status information
        """
        try:
            response = requests.get(f"{self.server_url}/health", timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            return {"status": "unhealthy", "error": str(e)}
