import os
import time
import uuid
import asyncio
from pathlib import Path
from typing import List, Dict, Optional, Any, Union

import httpx
from dotenv import load_dotenv
from entities_common import ValidationInterface
from entities.clients.file_processor import FileProcessor
from entities.clients.vector_store_manager import VectorStoreManager
from entities.services.logging_service import LoggingUtility

load_dotenv()
logging_utility = LoggingUtility()

class VectorStoreClientError(Exception):
    """Custom exception for VectorStoreClient errors."""
    pass

class VectorStoreClient:
    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        self.base_url = base_url or os.getenv("BASE_URL")
        self.api_key = api_key or os.getenv("API_KEY")
        if not self.base_url:
            raise VectorStoreClientError("BASE_URL must be provided either as an argument or in environment variables.")
        headers = {"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        self.api_client = httpx.Client(base_url=self.base_url, headers=headers)
        self.vector_manager = VectorStoreManager()

    def close(self):
        self.api_client.close()
        self.vector_manager.get_client().close()

    def _parse_response(self, response: httpx.Response) -> Any:
        try:
            return response.json()
        except Exception as e:
            logging_utility.error("Failed to parse response: %s", str(e))
            raise VectorStoreClientError("Invalid JSON response from API.")

    def _request_with_retries(self, method: str, url: str, **kwargs) -> httpx.Response:
        retries = 3
        for attempt in range(retries):
            try:
                response = self.api_client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as e:
                if response.status_code in {500, 503} and attempt < retries - 1:
                    logging_utility.warning("Retrying request (attempt %d)", attempt + 1)
                    time.sleep(2 ** attempt)
                    continue
                raise

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
        to a vector store, and optionally sync metadata to your backend.

        Parameters:
            file_path: Local path to the document (.pdf, .txt, .md, etc.).
            store_name: Name of the target Qdrant collection.
            user_metadata: Optional metadata for each chunk.
            source_url: Optional URL to associate with the document.
            embedding_model_name: Model for generating embeddings.
            chunk_size: Maximum chunk size.
            log_to_backend: If True, sync with backend DB.

        Returns:
            A dictionary containing:
              - store_name: The target collection.
              - chunks_processed: Number of chunks processed.
              - qdrant: Result from Qdrant upload.
              - db: Backend DB sync response.
        """
        file_path = Path(file_path)
        file_processor = FileProcessor(chunk_size=chunk_size)
        vector_store = VectorStoreManager()

        # Step 1: Preprocess and embed the file.
        processed = asyncio.run(file_processor.process_file(file_path))

        # Step 2: Prepare metadata.
        doc_metadata = user_metadata.copy() if user_metadata else {}
        if source_url:
            doc_metadata["url"] = source_url
        chunk_metadata = [
            file_processor._generate_chunk_metadata(processed, idx, doc_metadata)
            for idx in range(processed["metadata"]["chunks"])
        ]

        # Step 3: Upload to Qdrant.
        qdrant_result = vector_store.add_to_store(
            store_name=store_name,
            texts=processed["chunks"],
            vectors=processed["vectors"],
            metadata=chunk_metadata
        )

        # Step 4: Sync with backend (if enabled).
        db_result = None
        if log_to_backend:
            db_payload = {
                "name": store_name,
                "user_id": "generated_or_provided",  # Adjust as needed.
                "vector_size": processed.get("vector_size", 384),
                "distance_metric": "COSINE",
                "config": "{}",
                "texts": processed["chunks"],
                "vectors": processed["vectors"],
                "metadata": chunk_metadata
            }
            db_response = self._request_with_retries("POST", f"/v1/vector-stores/{store_name}/add", json=db_payload)
            db_result = self._parse_response(db_response)

        return {
            "store_name": store_name,
            "chunks_processed": len(processed["chunks"]),
            "qdrant": qdrant_result,
            "db": db_result
        }

    def create_vector_store(
        self, name: str, user_id: str, vector_size: int = 384,
        distance_metric: str = "COSINE", config: Optional[Dict[str, Any]] = None
    ) -> ValidationInterface.VectorStoreRead:
        # Qdrant operation.
        qdrant_result = self.vector_manager.create_store(name, vector_size, distance_metric)
        # DB sync payload.
        db_payload = {
            "name": name,
            "user_id": user_id,
            "vector_size": vector_size,
            "distance_metric": distance_metric,
            "config": config
        }
        db_response = self._request_with_retries("POST", "/v1/vector-stores", json=db_payload)
        response_data = self._parse_response(db_response)
        return ValidationInterface.VectorStoreRead.model_validate(response_data)

    def add_to_store(
        self, store_name: str, texts: List[str],
        vectors: List[List[float]], metadata: List[dict]
    ) -> Dict[str, Any]:
        # Qdrant operation.
        qdrant_result = self.vector_manager.add_to_store(store_name, texts, vectors, metadata)
        # DB sync payload.
        db_payload = {
            "store_name": store_name,
            "texts": texts,
            "vectors": vectors,
            "metadata": metadata
        }
        db_response = self._request_with_retries("POST", f"/v1/vector-stores/{store_name}/add", json=db_payload)
        return {
            "qdrant": qdrant_result,
            "db": self._parse_response(db_response)
        }

    def search_vector_store(
        self, store_name: str, query_vector: List[float],
        top_k: int = 5, page: int = 1, page_size: int = 10,
        score_threshold: float = 0.0
    ) -> Dict[str, Any]:
        offset = (page - 1) * page_size
        # Qdrant operation.
        qdrant_result = self.vector_manager.query_store(
            store_name=store_name,
            query_vector=query_vector,
            top_k=top_k,
            score_threshold=score_threshold,
            offset=offset,
            limit=page_size
        )
        # DB sync call (for auditing/monitoring).
        params = {
            "query_text": "query hidden from SDK here",
            "top_k": top_k,
            "page": page,
            "page_size": page_size
        }
        db_response = self._request_with_retries("GET", f"/v1/vector-stores/{store_name}/search", params=params)
        return {
            "qdrant": qdrant_result,
            "db": self._parse_response(db_response)
        }

    def delete_vector_store(self, store_name: str, permanent: bool = False) -> Dict[str, Any]:
        # Qdrant operation.
        qdrant_result = self.vector_manager.delete_store(store_name)
        # DB sync call.
        db_response = self._request_with_retries("DELETE", f"/v1/vector-stores/{store_name}",
                                                 params={"permanent": permanent})
        return {
            "qdrant": qdrant_result,
            "db": self._parse_response(db_response)
        }

    def delete_file_from_store(self, store_name: str, file_path: str) -> Dict[str, Any]:
        # Qdrant deletion via manager.
        qdrant_result = self.vector_manager.delete_file_from_store(store_name, file_path)
        # DB sync call.
        db_response = self._request_with_retries("DELETE", f"/v1/vector-stores/{store_name}/files",
                                                 params={"file_path": file_path})
        return {
            "qdrant": qdrant_result,
            "db": self._parse_response(db_response)
        }

    def list_store_files(self, store_name: str) -> List[str]:
        return self.vector_manager.list_store_files(store_name)

    def attach_vector_store_to_assistant(self, vector_store_id: str, assistant_id: str) -> bool:
        response = self._request_with_retries("POST", f"/v1/vector-stores/{vector_store_id}/attach/{assistant_id}")
        return bool(self._parse_response(response))

    def detach_vector_store_from_assistant(self, vector_store_id: str, assistant_id: str) -> bool:
        response = self._request_with_retries("POST", f"/v1/vector-stores/{vector_store_id}/detach/{assistant_id}")
        return bool(self._parse_response(response))

    def get_vector_stores_for_assistant(self, assistant_id: str) -> List[Any]:
        response = self._request_with_retries("GET", f"/v1/assistants/{assistant_id}/vector-stores")
        return self._parse_response(response)

    def get_stores_by_user(self, user_id: str) -> List[Any]:
        response = self._request_with_retries("GET", f"/v1/users/{user_id}/vector-stores")
        return self._parse_response(response)
