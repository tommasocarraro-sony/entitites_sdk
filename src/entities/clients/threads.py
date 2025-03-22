import os
from typing import List, Dict, Any, Optional

import httpx
from dotenv import load_dotenv
from pydantic import ValidationError

from ..schemas import UserCreate, UserRead, ThreadCreate, ThreadRead, ThreadUpdate, ThreadIds, \
    ThreadReadDetailed
from ..services.logging_service import LoggingUtility

load_dotenv()
# Initialize logging utility
logging_utility = LoggingUtility()


class ThreadsClient:
    def __init__(self, base_url=os.getenv("BASE_URL"), api_key=None):
        self.base_url = base_url
        self.api_key = api_key
        self.client = httpx.Client(base_url=base_url, headers={"Authorization": f"Bearer {api_key}"})
        logging_utility.info("ThreadsClient initialized with base_url: %s", self.base_url)

    def create_user(self, name: str) -> UserRead:
        logging_utility.info("Creating user with name: %s", name)
        user_data = UserCreate(name=name).model_dump()
        try:
            response = self.client.post("/v1/users", json=user_data)
            response.raise_for_status()
            created_user = response.json()
            validated_user = UserRead(**created_user)  # Validate data using Pydantic model
            logging_utility.info("User created successfully with id: %s", validated_user.id)
            return validated_user
        except ValidationError as e:
            logging_utility.error("Validation error: %s", e.json())
            raise ValueError(f"Validation error: {e}")
        except httpx.HTTPStatusError as e:
            logging_utility.error("HTTP error occurred while creating user: %s", str(e))
            raise
        except Exception as e:
            logging_utility.error("An error occurred while creating user: %s", str(e))
            raise

    def create_thread(self, participant_ids: List[str], meta_data: Optional[Dict[str, Any]] = None) -> ThreadRead:
        if meta_data is None:
            meta_data = {}

        thread_data = ThreadCreate(participant_ids=participant_ids, meta_data=meta_data).model_dump()
        logging_utility.info("Creating thread with %d participants", len(participant_ids))
        try:
            response = self.client.post("/v1/threads", json=thread_data)
            response.raise_for_status()
            created_thread = response.json()
            validated_thread = ThreadRead(**created_thread)  # Validate data using Pydantic model
            logging_utility.info("Thread created successfully with id: %s", validated_thread.id)
            return validated_thread
        except ValidationError as e:
            logging_utility.error("Validation error: %s", e.json())
            raise ValueError(f"Validation error: {e}")
        except httpx.HTTPStatusError as e:
            logging_utility.error("HTTP error occurred while creating thread: %s", str(e))
            logging_utility.error("Status code: %d, Response text: %s", e.response.status_code, e.response.text)
            raise
        except Exception as e:
            logging_utility.error("An error occurred while creating thread: %s", str(e))
            raise

    def retrieve_thread(self, thread_id: str) -> ThreadRead:
        logging_utility.info("Retrieving thread with id: %s", thread_id)
        try:
            response = self.client.get(f"/v1/threads/{thread_id}")
            response.raise_for_status()
            thread = response.json()
            validated_thread = ThreadRead(**thread)  # Validate data using Pydantic model
            logging_utility.info("Thread retrieved successfully")
            return validated_thread
        except ValidationError as e:
            logging_utility.error("Validation error: %s", e.json())
            raise ValueError(f"Validation error: {e}")
        except httpx.HTTPStatusError as e:
            logging_utility.error("HTTP error occurred while retrieving thread: %s", str(e))
            raise
        except Exception as e:
            logging_utility.error("An error occurred while retrieving thread: %s", str(e))
            raise

    def update_thread(self, thread_id: str, **updates) -> ThreadReadDetailed:

        #logging_utility(f"Updating thread with id: {thread_id}")

        try:
            validated_updates = ThreadUpdate(**updates)
            response = self.client.post(f"/v1/threads/{thread_id}", json=validated_updates.model_dump())
            response.raise_for_status()
            updated_thread = response.json()
            return ThreadReadDetailed(**updated_thread)
        except httpx.HTTPStatusError as e:
            logging_utility.error(f"HTTP error occurred while updating thread: {e}")
            logging_utility.error(f"Response content: {e.response.content}")
            raise
        except Exception as e:
            logging_utility.error(f"An error occurred while updating thread: {e}")
            raise

    def update_thread_metadata(self, thread_id: str, new_metadata: Dict[str, Any]) -> ThreadRead:
        """
        Updates the metadata for a specific thread by its ID.
        """
        logging_utility.info("Updating metadata for thread with id: %s", thread_id)
        try:
            # Retrieve the existing thread to ensure it exists and get current metadata
            thread = self.retrieve_thread(thread_id)
            current_metadata = thread.meta_data

            # Update the metadata
            current_metadata.update(new_metadata)

            # Send the update request
            return self.update_thread(thread_id, meta_data=current_metadata)
        except ValidationError as e:
            logging_utility.error("Validation error while updating thread metadata: %s", e.json())
            raise ValueError(f"Validation error: {e}")
        except httpx.HTTPStatusError as e:
            logging_utility.error("HTTP error occurred while updating thread metadata: %s", str(e))
            raise
        except Exception as e:
            logging_utility.error("An error occurred while updating thread metadata: %s", str(e))
            raise

    def list_threads(self, user_id: str) -> List[str]:
        logging_utility.info("Listing threads for user with id: %s", user_id)
        try:
            response = self.client.get(f"/v1/users/{user_id}/threads")
            response.raise_for_status()
            thread_ids = response.json()
            validated_thread_ids = ThreadIds(**thread_ids)  # Validate data using Pydantic model
            logging_utility.info("Retrieved %d thread ids", len(validated_thread_ids.thread_ids))
            return validated_thread_ids.thread_ids
        except ValidationError as e:
            logging_utility.error("Validation error: %s", e.json())
            raise ValueError(f"Validation error: {e}")
        except httpx.HTTPStatusError as e:
            logging_utility.error("HTTP error occurred while listing threads: %s", str(e))
            raise
        except Exception as e:
            logging_utility.error("An error occurred while listing threads: %s", str(e))
            raise

    def delete_thread(self, thread_id: str) -> bool:
        logging_utility.info("Deleting thread with id: %s", thread_id)
        try:
            response = self.client.delete(f"/v1/threads/{thread_id}")
            response.raise_for_status()
            logging_utility.info("Thread deleted successfully")
            return True  # Explicitly return True on success
        except httpx.HTTPStatusError as e:
            logging_utility.error(f"HTTP error occurred while deleting thread: {str(e)}")
            if e.response.status_code == 404:
                return False  # Explicit when thread not found
            raise

