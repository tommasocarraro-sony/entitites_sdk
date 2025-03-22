import os
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from ollama import Client as OllamaAPIClient

# Use relative imports for modules within your package.
from .clients.actions import ClientActionService
from .clients.assistants import AssistantsClient
from .clients.messages import ClientMessageService
from .clients.runs import ClientRunService
from .clients.threads import ThreadsClient
from .clients.tools import ClientToolClient as ClientToolService
from .clients.users import UsersClient
from .clients.inference import ClientInferenceService
from .clients.synchronous_inference_stream import SynchronousInferenceStream
from .services.logging_service import LoggingUtility

# Load environment variables from .env file.
load_dotenv()

# Initialize logging utility.
logging_utility = LoggingUtility()


class Entities:
    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        available_functions: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize the main client with configuration.
        Optionally, a configuration object can be injected to decouple from environment variables.
        """
        self.base_url = base_url or os.getenv('ASSISTANTS_BASE_URL', 'http://localhost:9000/')
        self.api_key = api_key or os.getenv('API_KEY', 'your_api_key')

        # Initialize the Ollama API client.
        self.ollama_client: OllamaAPIClient = OllamaAPIClient()

        logging_utility.info("Entities initialized with base_url: %s", self.base_url)

        # Lazy initialization caches for service instances.
        self._user_service: Optional[UsersClient] = None
        self._assistant_service: Optional[AssistantsClient] = None
        self._tool_service: Optional[ClientToolService] = None
        self._thread_service: Optional[ThreadsClient] = None
        self._message_service: Optional[ClientMessageService] = None
        self._run_service: Optional[ClientRunService] = None
        self._action_service: Optional[ClientActionService] = None
        self._inference_service: Optional[ClientInferenceService] = None
        self._synchronous_inference_stream: Optional[SynchronousInferenceStream] = None  # Added property

    @property
    def user_service(self) -> UsersClient:
        if self._user_service is None:
            self._user_service = UsersClient(base_url=self.base_url, api_key=self.api_key)
        return self._user_service

    @property
    def assistant_service(self) -> AssistantsClient:
        if self._assistant_service is None:
            self._assistant_service = AssistantsClient(base_url=self.base_url, api_key=self.api_key)
        return self._assistant_service

    @property
    def tool_service(self) -> ClientToolService:
        if self._tool_service is None:
            self._tool_service = ClientToolService()
        return self._tool_service

    @property
    def thread_service(self) -> ThreadsClient:
        if self._thread_service is None:
            self._thread_service = ThreadsClient(base_url=self.base_url, api_key=self.api_key)
        return self._thread_service

    @property
    def message_service(self) -> ClientMessageService:
        if self._message_service is None:
            self._message_service = ClientMessageService(base_url=self.base_url, api_key=self.api_key)
        return self._message_service

    @property
    def run_service(self) -> ClientRunService:
        if self._run_service is None:
            self._run_service = ClientRunService()
        return self._run_service

    @property
    def action_service(self) -> ClientActionService:
        if self._action_service is None:
            self._action_service = ClientActionService()
        return self._action_service

    @property
    def inference_service(self) -> ClientInferenceService:
        """
        Exposes the asynchronous inference client via the public interface.
        """
        if self._inference_service is None:
            self._inference_service = ClientInferenceService(base_url=self.base_url, api_key=self.api_key)
        return self._inference_service

    @property
    def synchronous_inference_stream(self) -> SynchronousInferenceStream:
        """
        Exposes the synchronous inference stream wrapper via the public interface.
        """
        if self._synchronous_inference_stream is None:
            self._synchronous_inference_stream = SynchronousInferenceStream(self.inference_service)
        return self._synchronous_inference_stream
