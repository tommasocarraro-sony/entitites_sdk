import os
from typing import Dict, Any, Optional, BinaryIO

import httpx
from dotenv import load_dotenv
from pydantic import ValidationError

from ..schemas.file_service import FileResponse, FileUploadRequest
from ..services.logging_service import LoggingUtility

load_dotenv()
# Initialize logging utility
logging_utility = LoggingUtility()


class FileClient:
    def __init__(self, base_url=os.getenv("BASE_URL"), api_key=None):
        self.base_url = base_url
        self.api_key = api_key
        self.client = httpx.Client(base_url=base_url, headers={"Authorization": f"Bearer {api_key}"})
        logging_utility.info("FileClient initialized with base_url: %s", self.base_url)

    def upload_file(self, file_path: str, user_id: str, file_type: Optional[str] = None,
                    description: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> FileResponse:
        """
        Upload a file to the server.

        Args:
            file_path: Path to the file to upload
            user_id: ID of the user uploading the file
            file_type: Type of the file (optional)
            description: Description of the file (optional)
            metadata: Additional metadata for the file (optional)

        Returns:
            FileResponse: The response from the server with file metadata
        """
        logging_utility.info("Uploading file: %s for user: %s", file_path, user_id)

        try:
            # Prepare the file upload request
            file_name = os.path.basename(file_path)

            # Create the request data
            request_data = {
                "user_id": user_id
            }

            if file_type:
                request_data["file_type"] = file_type
            if description:
                request_data["description"] = description
            if metadata:
                request_data["metadata"] = metadata

            # Prepare the file for upload
            with open(file_path, 'rb') as file_object:
                files = {
                    'file': (file_name, file_object, 'application/octet-stream')
                }

                # Make the request
                response = self.client.post(
                    "/v1/uploads",
                    data=request_data,
                    files=files
                )
                response.raise_for_status()

                # Parse and validate the response
                file_data = response.json()
                validated_response = FileResponse.model_validate(file_data)
                logging_utility.info("File uploaded successfully with id: %s", validated_response.id)
                return validated_response

        except ValidationError as e:
            logging_utility.error("Validation error: %s", e.json())
            raise ValueError(f"Validation error: {e}")
        except httpx.HTTPStatusError as e:
            logging_utility.error("HTTP error occurred while uploading file: %s", str(e))
            raise
        except Exception as e:
            logging_utility.error("An error occurred while uploading file: %s", str(e))
            raise

    def upload_file_object(self, file_object: BinaryIO, file_name: str, user_id: str,
                           file_type: Optional[str] = None, description: Optional[str] = None,
                           metadata: Optional[Dict[str, Any]] = None) -> FileResponse:
        """
        Upload a file object to the server.

        Args:
            file_object: File-like object to upload
            file_name: Name to give the file
            user_id: ID of the user uploading the file
            file_type: Type of the file (optional)
            description: Description of the file (optional)
            metadata: Additional metadata for the file (optional)

        Returns:
            FileResponse: The response from the server with file metadata
        """
        logging_utility.info("Uploading file object: %s for user: %s", file_name, user_id)

        try:
            # Create the request data
            request_data = {
                "user_id": user_id
            }

            if file_type:
                request_data["file_type"] = file_type
            if description:
                request_data["description"] = description
            if metadata:
                request_data["metadata"] = metadata

            # Prepare the file for upload
            files = {
                'file': (file_name, file_object, 'application/octet-stream')
            }

            # Make the request
            response = self.client.post(
                "/v1/uploads",
                data=request_data,
                files=files
            )
            response.raise_for_status()

            # Parse and validate the response
            file_data = response.json()
            validated_response = FileResponse.model_validate(file_data)
            logging_utility.info("File uploaded successfully with id: %s", validated_response.id)
            return validated_response

        except ValidationError as e:
            logging_utility.error("Validation error: %s", e.json())
            raise ValueError(f"Validation error: {e}")
        except httpx.HTTPStatusError as e:
            logging_utility.error("HTTP error occurred while uploading file: %s", str(e))
            raise
        except Exception as e:
            logging_utility.error("An error occurred while uploading file: %s", str(e))
            raise