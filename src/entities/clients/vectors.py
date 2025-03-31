import os
import time
from typing import List, Dict, Any, Optional
import httpx
from dotenv import load_dotenv
from pydantic import ValidationError
from entities_common import UtilsInterface

from entities_common import ValidationInterface

# Load environment variables
load_dotenv()


logging_utility = UtilsInterface.LoggingUtility()


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
        self.client = httpx.Client(base_url=self.base_url, headers=headers)

        logging_utility.info("VectorStoreClient initialized with base_url: %s", self.base_url)

    def close(self):
        """Closes the HTTP client session."""
        self.client.close()

    def _parse_response(self, response: httpx.Response):
        """Parses JSON responses safely."""
        try:
            return response.json()
        except httpx.HTTPStatusError as e:
            logging_utility.error("API returned HTTP error: %s", str(e))
            raise
        except httpx.DecodingError:
            logging_utility.error("Failed to decode JSON response: %s", response.text)
            raise VectorStoreClientError("Invalid JSON response from API.")

    def _request_with_retries(self, method: str, url: str, **kwargs) -> httpx.Response:
        """Handles retries for transient failures."""
        retries = 3
        for attempt in range(retries):
            try:
                response = self.client.request(method, url, **kwargs)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as e:
                if response.status_code in {500, 503} and attempt < retries - 1:
                    logging_utility.warning("Retrying request due to server error (attempt %d)", attempt + 1)
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise

    def create_vector_store(
        self,
        name: str,
        user_id: str,
        vector_size: int,
        distance_metric: str,
        config: Optional[Dict[str, Any]] = None
    ) -> ValidationInterface.VectorStoreRead:
        """Creates a new vector store."""
        store_data = {
            "name": name,
            "user_id": user_id,
            "vector_size": vector_size,
            "distance_metric": distance_metric,
            "config": config
        }
        try:
            validated_data = ValidationInterface.VectorStoreCreate(**store_data)
            logging_utility.info("Creating vector store with name: %s", name)

            response = self._request_with_retries("POST", "/v1/vector-stores", json=validated_data.model_dump())
            created_store = self._parse_response(response)

            validated_response = ValidationInterface.VectorStoreRead(**created_store)
            logging_utility.info("Vector store created successfully with id: %s", validated_response.id)
            return validated_response
        except ValidationError as e:
            logging_utility.error("Validation error: %s", e.json())
            raise VectorStoreClientError(f"Validation error: {e}")

    def add_to_store(
        self,
        store_name: str,
        texts: List[str],
        vectors: List[List[float]],
        metadata: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Adds batches of texts, vectors, and metadata to a vector store."""
        request_data = {
            "texts": texts,
            "vectors": vectors,
            "metadata": metadata
        }
        try:
            validated_data = ValidationInterface.VectorStoreAddRequest(**request_data)
            logging_utility.info("Adding data to vector store: %s", store_name)

            response = self._request_with_retries("POST", f"/v1/vector-stores/{store_name}/add", json=validated_data.model_dump())
            return self._parse_response(response)
        except ValidationError as e:
            logging_utility.error("Validation error: %s", e.json())
            raise VectorStoreClientError(f"Validation error: {e}")

    def search_vector_store(
        self,
        store_name: str,
        query_text: str,
        top_k: int = 5,
        page: int = 1,
        page_size: int = 10
    ) -> List[ValidationInterface.VectorStoreSearchResult]:
        """Searches a vector store for a given query."""
        params = {
            "query_text": query_text,
            "top_k": top_k,
            "page": page,
            "page_size": page_size
        }
        logging_utility.info("Searching vector store: %s with query: %s", store_name, query_text)
        response = self._request_with_retries("GET", f"/v1/vector-stores/{store_name}/search", params=params)
        search_results = self._parse_response(response)

        validated_results = []
        for result in search_results:
            try:
                validated_result = ValidationInterface.VectorStoreSearchResult(**result)
                validated_results.append(validated_result)
            except ValidationError as e:
                logging_utility.error("Validation error in search result: %s", e.json())
                raise VectorStoreClientError(f"Validation error in search result: {e}")
        return validated_results

    def delete_vector_store(self, store_name: str, permanent: bool = False) -> Dict[str, Any]:
        """Deletes a vector store."""
        logging_utility.info("Deleting vector store: %s, permanent=%s", store_name, permanent)
        params = {"permanent": permanent}
        response = self._request_with_retries("DELETE", f"/v1/vector-stores/{store_name}", params=params)
        return self._parse_response(response)

    def list_store_files(self, store_name: str) -> List[str]:
        """Lists files in a given vector store."""
        logging_utility.info("Listing files in vector store: %s", store_name)
        response = self._request_with_retries("GET", f"/v1/vector-stores/{store_name}/files")
        return self._parse_response(response)

    def delete_file_from_store(self, store_name: str, file_path: str) -> Dict[str, Any]:
        """Deletes a specific file from a vector store."""
        logging_utility.info("Deleting file from vector store: %s, file_path: %s", store_name, file_path)
        params = {"file_path": file_path}
        response = self._request_with_retries("DELETE", f"/v1/vector-stores/{store_name}/files", params=params)
        return self._parse_response(response)

    def add_file_to_store(
        self,
        store_name: str,
        file_path: str,
        user_metadata: Optional[Dict[str, Any]] = None,
        source_url: Optional[str] = None
    ) -> ValidationInterface.ProcessOutput:
        """Uploads a file to a vector store."""
        logging_utility.info("Uploading file to vector store: %s, file_path: %s", store_name, file_path)
        files = {"file": open(file_path, "rb")}
        data = {}
        if user_metadata:
            data["user_metadata"] = user_metadata
        if source_url:
            data["source_url"] = source_url

        response = self._request_with_retries("POST", f"/v1/vector-stores/{store_name}/files", data=data, files=files)
        files["file"].close()
        processed_output = self._parse_response(response)
        try:
            validated_output = ValidationInterface.ProcessOutput(**processed_output)
            return validated_output
        except ValidationError as e:
            logging_utility.error("Validation error in file upload response: %s", e.json())
            raise VectorStoreClientError(f"Validation error in file upload response: {e}")

    def attach_vector_store_to_assistant(self, vector_store_id: str, assistant_id: str) -> bool:
        """Associates a vector store with an assistant."""
        logging_utility.info("Attaching vector store %s to assistant %s", vector_store_id, assistant_id)
        response = self._request_with_retries("POST", f"/v1/vector-stores/{vector_store_id}/attach/{assistant_id}")
        result = self._parse_response(response)
        return result if isinstance(result, bool) else bool(result)

    def detach_vector_store_from_assistant(self, vector_store_id: str, assistant_id: str) -> bool:
        """Dissociates a vector store from an assistant."""
        logging_utility.info("Detaching vector store %s from assistant %s", vector_store_id, assistant_id)
        response = self._request_with_retries("POST", f"/v1/vector-stores/{vector_store_id}/detach/{assistant_id}")
        result = self._parse_response(response)
        return result if isinstance(result, bool) else bool(result)

    def get_vector_stores_for_assistant(self, assistant_id: str) -> List[ValidationInterface.VectorStoreRead]:
        """Retrieves all vector stores associated with a given assistant."""
        logging_utility.info("Retrieving vector stores for assistant: %s", assistant_id)
        response = self._request_with_retries("GET", f"/v1/assistants/{assistant_id}/vector-stores")
        stores = self._parse_response(response)

        validated_stores = []
        for store in stores:
            try:
                validated_store = ValidationInterface.VectorStoreRead(**store)
                validated_stores.append(validated_store)
            except ValidationError as e:
                logging_utility.error("Validation error in vector store: %s", e.json())
                raise VectorStoreClientError(f"Validation error in vector store: {e}")
        return validated_stores

    def get_stores_by_user(self, user_id: str) -> List[ValidationInterface.VectorStoreRead]:
        """Retrieves all vector stores for a given user."""
        logging_utility.info("Retrieving vector stores for user: %s", user_id)
        response = self._request_with_retries("GET", f"/v1/users/{user_id}/vector-stores")
        stores = self._parse_response(response)

        validated_stores = []
        for store in stores:
            try:
                validated_store = ValidationInterface.VectorStoreRead(**store)
                validated_stores.append(validated_store)
            except ValidationError as e:
                logging_utility.error("Validation error in vector store: %s", e.json())
                raise VectorStoreClientError(f"Validation error in vector store: {e}")
        return validated_stores

    def health_check(self, deep_check: bool = False) -> Dict[str, Any]:
        """Performs a health check on the vector store system."""
        logging_utility.info("Performing health check for vector stores")
        params = {"deep_check": deep_check}
        response = self._request_with_retries("GET", "/v1/vector-stores/health", params=params)
        return self._parse_response(response)

    def store_message(self, store_name: str, message: Dict[str, Any], role: str = "user") -> Dict[str, Any]:
        """Stores a message in a vector store with an assigned role."""
        logging_utility.info("Storing message in vector store: %s with role: %s", store_name, role)
        payload = {"message": message, "role": role}
        response = self._request_with_retries("POST", f"/v1/vector-stores/{store_name}/messages", json=payload)
        return self._parse_response(response)
