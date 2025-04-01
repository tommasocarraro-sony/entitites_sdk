# src/entities/clients/vector_store_manager.py
import os
import time
import uuid
import httpx
from typing import List, Dict, Optional

from dotenv import load_dotenv
from .base_vector_store import (
    BaseVectorStore,
    StoreExistsError,
    StoreNotFoundError,
    VectorStoreError,
)
from entities.services.logging_service import LoggingUtility

load_dotenv()
logging_utility = LoggingUtility()


class VectorStoreManager(BaseVectorStore):
    def __init__(self):
        self.base_url = os.getenv("QDRANT_URL", "http://qdrant:6333")
        self.client = httpx.Client(base_url=self.base_url, timeout=10.0)
        self.active_stores: Dict[str, dict] = {}
        logging_utility.info(f"Initialized HTTP-based VectorStoreManager. Source: {__file__}")

    def _generate_vector_id(self) -> str:
        return str(uuid.uuid4())

    def create_store(self, store_name: str, vector_size: int = 384, distance: str = "COSINE") -> dict:
        if store_name in self.active_stores:
            raise StoreExistsError(f"Store {store_name} exists")

        try:
            payload = {
                "vectors": {
                    "size": vector_size,
                    "distance": distance.upper(),
                }
            }
            response = self.client.put(f"/collections/{store_name}", json=payload)
            response.raise_for_status()

            self.active_stores[store_name] = {
                "created_at": int(time.time()),
                "vector_size": vector_size,
                "distance": distance
            }
            return {"name": store_name, "status": "created"}
        except httpx.HTTPError as e:
            logging_utility.error(f"Create store HTTP error: {str(e)}")
            raise VectorStoreError(f"Store creation failed: {str(e)}")

    def create_store_for_file_type(self, file_type: str) -> dict:
        if file_type != "text":
            raise ValueError("Only text files are currently supported")

        store_name = f"text_store_{int(time.time())}"
        return self.create_store(store_name, vector_size=384, distance="COSINE")

    def _validate_vectors(self, vectors: List[List[float]]):
        if not vectors:
            raise ValueError("Empty vectors list")

        expected_size = len(vectors[0])
        for i, vec in enumerate(vectors):
            if len(vec) != expected_size:
                raise ValueError(f"Vector {i} has size {len(vec)} (expected {expected_size})")
            if not all(isinstance(v, float) for v in vec):
                raise TypeError(f"Vector {i} contains non-float values")

    def add_to_store(self, store_name: str, texts: List[str],
                     vectors: List[List[float]], metadata: List[dict]):
        self._validate_vectors(vectors)
        points = []
        for text, vector, meta in zip(texts, vectors, metadata):
            points.append({
                "id": str(uuid.uuid4()),
                "vector": vector,
                "payload": {
                    "text": text,
                    "metadata": meta
                }
            })
        try:
            res = self.client.put(f"/collections/{store_name}/points", json={"points": points})
            res.raise_for_status()
            return res.json()
        except httpx.HTTPError as e:
            logging_utility.error(f"Add to store failed: {str(e)}")
            raise VectorStoreError(f"Insertion failed: {str(e)}")

    def query_store(self, store_name: str, query_vector: List[float],
                    top_k: int = 5, filters: Optional[dict] = None,
                    score_threshold: float = 0.0, offset: int = 0,
                    limit: Optional[int] = None, **kwargs) -> List[dict]:
        try:
            payload = {
                "vector": query_vector,
                "top": limit or top_k,
                "offset": offset,
                "with_payload": True,
                "with_vector": False,
                "score_threshold": score_threshold
            }
            if filters:
                payload["filter"] = filters
            res = self.client.post(f"/collections/{store_name}/points/search", json=payload)
            res.raise_for_status()
            results = res.json()["result"]
            return [{
                "id": r["id"],
                "score": r["score"],
                "text": r["payload"].get("text"),
                "metadata": r["payload"].get("metadata", {})
            } for r in results]
        except httpx.HTTPError as e:
            logging_utility.error(f"Query failed: {str(e)}")
            raise VectorStoreError(f"Query failed: {str(e)}")

    def delete_file_from_store(self, store_name: str, file_path: str) -> dict:
        try:
            payload = {
                "filter": {
                    "must": [
                        {
                            "key": "metadata.source",
                            "match": {"value": file_path}
                        }
                    ]
                }
            }
            res = self.client.post(f"/collections/{store_name}/points/delete", json=payload)
            res.raise_for_status()
            return {"deleted_file": file_path, "store_name": store_name, "status": "success"}
        except httpx.HTTPError as e:
            logging_utility.error(f"File deletion failed: {str(e)}")
            raise VectorStoreError(f"File deletion failed: {str(e)}")

    def delete_store(self, store_name: str) -> dict:
        if store_name not in self.active_stores:
            raise StoreNotFoundError(f"Store {store_name} not found")
        try:
            res = self.client.delete(f"/collections/{store_name}")
            res.raise_for_status()
            del self.active_stores[store_name]
            return {"name": store_name, "status": "deleted"}
        except httpx.HTTPError as e:
            logging_utility.error(f"Delete failed: {str(e)}")
            raise VectorStoreError(f"Store deletion failed: {str(e)}")

    def get_store_info(self, store_name: str) -> dict:
        if store_name not in self.active_stores:
            raise StoreNotFoundError(f"Store {store_name} not found")
        try:
            res = self.client.get(f"/collections/{store_name}")
            res.raise_for_status()
            info = res.json()["result"]
            return {
                "name": store_name,
                "status": "active",
                "vectors_count": info["points_count"],
                "configuration": info["config"]["params"]["vectors"],
                "created_at": self.active_stores[store_name]["created_at"]
            }
        except httpx.HTTPError as e:
            logging_utility.error(f"Store info failed: {str(e)}")
            raise VectorStoreError(f"Info retrieval failed: {str(e)}")

    def list_store_files(self, store_name: str) -> List[str]:
        try:
            scroll_url = f"/collections/{store_name}/points/scroll"
            payload = {"limit": 100, "with_payload": ["metadata.source"]}
            seen = set()
            while True:
                res = self.client.post(scroll_url, json=payload)
                res.raise_for_status()
                result = res.json()["result"]
                for point in result["points"]:
                    source = point.get("payload", {}).get("metadata", {}).get("source")
                    if source:
                        seen.add(source)
                if not result.get("next_page_offset"):
                    break
                payload["offset"] = result["next_page_offset"]
            return sorted(seen)
        except httpx.HTTPError as e:
            logging_utility.error(f"List store files failed: {str(e)}")
            raise VectorStoreError(f"List files failed: {str(e)}")

    def get_client(self):
        return self.client

    def get_point_by_id(self, store_name: str, point_id: str) -> dict:
        try:
            res = self.client.get(f"/collections/{store_name}/points/{point_id}")
            res.raise_for_status()
            return res.json()
        except httpx.HTTPError as e:
            logging_utility.error(f"Get point by ID failed: {str(e)}")
            raise VectorStoreError(f"Fetch failed: {str(e)}")

    def health_check(self) -> bool:
        try:
            res = self.client.get("/collections")
            res.raise_for_status()
            return "collections" in res.json()
        except httpx.HTTPError as e:
            logging_utility.error(f"Health check failed: {str(e)}")
            return False
