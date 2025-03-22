import json
import os
import time
from typing import Optional, AsyncGenerator
import httpx
from pydantic import ValidationError
from ..schemas import StreamRequest
from ..services.logging_service import LoggingUtility
import asyncio
from dotenv import load_dotenv

load_dotenv()

logging_utility = LoggingUtility()


class InferenceClient:
    """
    Client-side service for interacting with the completions endpoint.

    Exposes:
      - create_completion_sync(...): a synchronous wrapper that blocks until the response is aggregated.
      - stream_inference_response(...): an async generator for real-time streaming.
    """
    def __init__(self, base_url=os.getenv("BASE_URL"), api_key=None):

        self.base_url = base_url
        self.api_key = api_key
        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self.client = httpx.Client(base_url=base_url, headers=headers)
        logging_utility.info("InferenceClient initialized with base_url: %s", self.base_url)

    def create_completion_sync(
            self,
            provider: str,
            model: str,
            thread_id: str,
            message_id: str,
            run_id: str,
            assistant_id: str,
            user_content: Optional[str] = None,
            api_key: Optional[str] = None
    ) -> dict:
        """
        Synchronously aggregates the streaming completions result and returns the JSON completion.
        Internally, it uses the asynchronous stream_inference_response method in a background thread.
        """
        payload = {
            "provider": provider,
            "model": model,
            "api_key": api_key,
            "thread_id": thread_id,
            "message_id": message_id,
            "run_id": run_id,
            "assistant_id": assistant_id,
        }
        if user_content:
            payload["content"] = user_content

        try:
            validated_payload = StreamRequest(**payload)
        except ValidationError as e:
            logging_utility.error("Payload validation error: %s", e.json())
            raise ValueError(f"Payload validation error: {e}")

        logging_utility.info("Sending completions request (sync wrapper): %s", validated_payload.dict())

        # Use the asynchronous streaming endpoint and aggregate its output.
        async def aggregate():
            final_text = ""
            async for chunk in self.stream_inference_response(
                    provider=provider,
                    model=model,
                    thread_id=thread_id,
                    message_id=message_id,
                    run_id=run_id,
                    assistant_id=assistant_id,
                    user_content=user_content,
                    api_key=api_key):
                # Here we assume each chunk is a dict.
                final_text += chunk.get("content", "")
            return final_text

        # Run the aggregation in a background thread
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            final_content = loop.run_until_complete(aggregate())
        finally:
            loop.close()

        # Construct the completions response
        completions_response = {
            "id": f"chatcmpl-{run_id}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": final_content,
                    },
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": 0,
                "completion_tokens": len(final_content.split()),
                "total_tokens": len(final_content.split())
            }
        }
        return completions_response

    async def stream_inference_response(
            self,
            provider: str,
            model: str,
            thread_id: str,
            message_id: str,
            run_id: str,
            assistant_id: str,
            user_content: Optional[str] = None,
            api_key: Optional[str] = None
    ) -> AsyncGenerator[dict, None]:
        """
        Initiates an asynchronous streaming request to the completions endpoint and yields each response chunk as a dict.
        """
        payload = {
            "provider": provider,
            "model": model,
            "api_key": api_key,
            "thread_id": thread_id,
            "message_id": message_id,
            "run_id": run_id,
            "assistant_id": assistant_id,
        }
        if user_content:
            payload["content"] = user_content

        try:
            validated_payload = StreamRequest(**payload)
        except ValidationError as e:
            logging_utility.error("Payload validation error: %s", e.json())
            raise ValueError(f"Payload validation error: {e}")

        logging_utility.info("Sending streaming inference request: %s", validated_payload.dict())

        async with httpx.AsyncClient(base_url=self.base_url) as async_client:
            if self.api_key:
                async_client.headers["Authorization"] = f"Bearer {self.api_key}"
            try:
                async with async_client.stream("POST", "/v1/completions", json=validated_payload.dict()) as response:
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if line.startswith("data:"):
                            data_str = line[len("data:"):].strip()
                            if data_str == "[DONE]":
                                break
                            try:
                                chunk = json.loads(data_str)
                                yield chunk
                            except json.JSONDecodeError as json_exc:
                                logging_utility.error("Error decoding JSON from stream: %s", str(json_exc))
                                continue
            except httpx.HTTPStatusError as e:
                logging_utility.error("HTTP error during streaming completions: %s", str(e))
                raise
            except Exception as e:
                logging_utility.error("Unexpected error during streaming completions: %s", str(e))
                raise

    def close(self):
        """
        Closes the underlying synchronous HTTP client.
        """
        self.client.close()
