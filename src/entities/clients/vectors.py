import asyncio
import os
from pathlib import Path
from typing import List, Dict, Optional, Any, Union

import httpx

from entities.clients.file_processor import FileProcessor
from entities.clients.vector_store_manager import VectorStoreManager
from entities.services.logging_service import LoggingUtility

logging_utility = LoggingUtility()

class VectorStoreClient:
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        self.base_url = base_url or os.getenv("BASE_URL")
        self.api_key = api_key or os.getenv("API_KEY")
        if not self.base_url:
            raise Exception("BASE_URL must be provided either as an argument or in environment variables.")

        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        self.api_client = httpx.Client(base_url=self.base_url, headers=headers)

        self.vector_manager = VectorStoreManager()



    def process_and_upload_file(
            self,
            file_path: Union[str, Path],
            store_name: str,
            user_metadata: Optional[Dict[str, Any]] = None,
            source_url: Optional[str] = None,
            embedding_model_name: str = "paraphrase-MiniLM-L6-v2",
            chunk_size: int = 512,
            log_to_backend: bool = True
    ) -> Dict[str, Any]:
        """
        Preprocess a document (PDF or text), generate embeddings, upload them
        to a vector store, and optionally log metadata to your backend.

        Parameters:
        ----------
        file_path : Union[str, Path]
            Local path to the document to be embedded. Can be `.pdf`, `.txt`, `.md`, or `.rst`.

        store_name : str
            Name of the target Qdrant collection.

        user_metadata : Optional[Dict[str, Any]]
            Optional user-defined metadata that will be included with each chunk.

        source_url : Optional[str]
            URL to associate with the document (useful for web-sourced files).

        embedding_model_name : str
            SentenceTransformer model to use for embedding. Defaults to "paraphrase-MiniLM-L6-v2".

        chunk_size : int
            Maximum token-length per chunk (in characters). Final chunk size respects model tokenizer limits.

        log_to_backend : bool
            If True, the chunk metadata and embeddings will also be posted to your backend
            `/v1/vector-stores/{store_name}/add` endpoint for DB syncing.

        Returns:
        --------
        Dict[str, Any]
            {
                "store_name": str,
                "chunks_processed": int,
                "qdrant": Dict[str, Any],
                "db": Dict[str, Any] | None
            }

        Raises:
        -------
        Exception on I/O, embedding, or HTTP upload failures.
        """
        file_path = Path(file_path)
        file_processor = FileProcessor(chunk_size=chunk_size)
        vector_store = VectorStoreManager()

        # --- Step 1: Preprocess & embed ---
        processed = asyncio.run(file_processor.process_file(file_path))

        # --- Step 2: Prepare metadata ---
        doc_metadata = user_metadata.copy() if user_metadata else {}
        if source_url:
            doc_metadata["url"] = source_url

        chunk_metadata = [
            file_processor._generate_chunk_metadata(processed, idx, doc_metadata)
            for idx in range(processed["metadata"]["chunks"])
        ]

        # --- Step 3: Upload to Qdrant ---
        qdrant_result = vector_store.add_to_store(
            store_name=store_name,
            texts=processed["chunks"],
            vectors=processed["vectors"],
            metadata=chunk_metadata
        )

        # --- Step 4: Sync with backend (if enabled) ---
        db_result = None
        if log_to_backend:
            db_payload = {
                "store_name": store_name,
                "texts": processed["chunks"],
                "vectors": processed["vectors"],
                "metadata": chunk_metadata
            }

            db_response = self.api_client.post(
                f"/v1/vector-stores/{store_name}/add",
                json=db_payload
            )
            db_response.raise_for_status()
            db_result = db_response.json()

        return {
            "store_name": store_name,
            "chunks_processed": len(processed["chunks"]),
            "qdrant": qdrant_result,
            "db": db_result
        }

    def create_vector_store(
        self, name: str, user_id: str, vector_size: int = 384,
        distance_metric: str = "COSINE", config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        # Qdrant
        qdrant_result = self.vector_manager.create_store(name, vector_size, distance_metric)

        # DB sync
        db_payload = {
            "name": name,
            "user_id": user_id,
            "vector_size": vector_size,
            "distance_metric": distance_metric,
            "config": config
        }
        db_response = self.api_client.post("/v1/vector-stores", json=db_payload)
        db_response.raise_for_status()

        return {
            "qdrant": qdrant_result,
            "db": db_response.json()
        }

    def add_to_store(
        self, store_name: str, texts: List[str],
        vectors: List[List[float]], metadata: List[dict]
    ) -> Dict[str, Any]:
        # Qdrant
        qdrant_result = self.vector_manager.add_to_store(store_name, texts, vectors, metadata)

        # DB sync
        db_payload = {
            "store_name": store_name,
            "texts": texts,
            "vectors": vectors,
            "metadata": metadata
        }
        db_response = self.api_client.post(f"/v1/vector-stores/{store_name}/add", json=db_payload)
        db_response.raise_for_status()

        return {
            "qdrant": qdrant_result,
            "db": db_response.json()
        }

    def search_vector_store(
        self, store_name: str, query_vector: List[float],
        top_k: int = 5, page: int = 1, page_size: int = 10,
        score_threshold: float = 0.0
    ) -> Dict[str, Any]:
        offset = (page - 1) * page_size

        # Qdrant
        qdrant_result = self.vector_manager.query_store(
            store_name=store_name,
            query_vector=query_vector,
            top_k=top_k,
            score_threshold=score_threshold,
            offset=offset,
            limit=page_size
        )

        # DB call (optional: for monitoring, auditing, fallback)
        params = {
            "query_text": "query hidden from SDK here",
            "top_k": top_k,
            "page": page,
            "page_size": page_size
        }
        db_response = self.api_client.get(f"/v1/vector-stores/{store_name}/search", params=params)
        db_response.raise_for_status()

        return {
            "qdrant": qdrant_result,
            "db": db_response.json()
        }

    def delete_vector_store(self, store_name: str, permanent: bool = False) -> Dict[str, Any]:
        # Qdrant
        qdrant_result = self.vector_manager.delete_store(store_name)

        # DB update
        db_response = self.api_client.delete(
            f"/v1/vector-stores/{store_name}", params={"permanent": permanent}
        )
        db_response.raise_for_status()

        return {
            "qdrant": qdrant_result,
            "db": db_response.json()
        }

    def delete_file_from_store(self, store_name: str, file_path: str) -> Dict[str, Any]:
        qdrant_result = self.vector_manager.delete_file_from_store(store_name, file_path)

        db_response = self.api_client.delete(
            f"/v1/vector-stores/{store_name}/files", params={"file_path": file_path}
        )
        db_response.raise_for_status()

        return {
            "qdrant": qdrant_result,
            "db": db_response.json()
        }

    def list_store_files(self, store_name: str) -> List[str]:
        return self.vector_manager.list_store_files(store_name)

    def close(self):
        self.api_client.close()
        self.vector_manager.get_client().close()

