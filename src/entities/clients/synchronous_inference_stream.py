import asyncio


class SynchronousInferenceStream:
    """
    A wrapper class for synchronous real-time streaming inference.
    Handles all async logic internally and exposes a clean synchronous interface.
    """

    def __init__(self, inference_service):
        """
        Initialize the wrapper with the required inference service.
        """
        self.inference_service = inference_service
        self.user_id = None
        self.thread_id = None
        self.assistant_id = None
        self.message_id = None
        self.run_id = None

    def setup(self, user_id: str, thread_id: str, assistant_id: str, message_id: str, run_id: str):
        """
        Handles the setup of user, thread, assistant, message, and run with provided IDs.
        """
        self.user_id = user_id
        self.thread_id = thread_id
        self.assistant_id = assistant_id
        self.message_id = message_id
        self.run_id = run_id

    def stream_chunks(self, provider: str, model: str) -> iter:
        """
        Synchronously streams and yields chunks of inference results in real-time.

        Args:
            provider (str): The inference provider name.
            model (str): The model identifier.

        Yields:
            dict: Each chunk as it is received from the streaming inference response.
        """
        async def _stream_chunks_async():
            """
            Internal async generator to stream chunks using the asynchronous inference service.
            """
            async for chunk in self.inference_service.stream_inference_response(
                provider=provider,
                model=model,
                thread_id=self.thread_id,
                message_id=self.message_id,
                run_id=self.run_id,
                assistant_id=self.assistant_id
            ):
                yield chunk

        # Run the async generator in a synchronous context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            generator = _stream_chunks_async()

            while True:
                try:
                    # Fetch the next chunk from the async generator
                    chunk = loop.run_until_complete(generator.__anext__())
                    yield chunk
                except StopAsyncIteration:
                    # If the generator is exhausted, stop iteration
                    break
        finally:
            loop.close()

    def close(self):
        """
        Closes the inference service to release resources.
        """
        self.inference_service.close()
