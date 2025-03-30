import httpx
import json
from typing import Callable, Optional


class RunMonitorClient:
    """
    SDK-facing class for monitoring a run by streaming events from the API endpoint.

    Usage:
        monitor = RunMonitorClient(base_url="http://localhost:9000", api_key="your_api_key")
        monitor.start(run_id="run_xyz", callback=my_callback)
    """

    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.client = httpx.Client(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {self.api_key}"} if self.api_key else {}
        )

    def start(self, run_id: str, callback: Callable[[str, dict], None]):
        """
        Start monitoring a run by making a POST to the monitoring endpoint and streaming the response.

        Args:
            run_id (str): The run ID to monitor.
            callback (callable): A function accepting (event_type, event_data) to handle events.
        """
        try:
            # Initiate monitoring by calling the API endpoint.
            # Our API endpoint (/v1/events/monitor) expects a JSON payload with run_id.
            response = self.client.post("/v1/events/monitor", json={"run_id": run_id})
            response.raise_for_status()

            # Now, we assume the endpoint responds with a streaming body
            # in newline-delimited JSON (ndjson).
            with self.client.stream("GET", "/v1/events/stream", params={"run_id": run_id}) as stream_response:
                for line in stream_response.iter_lines():
                    if not line:
                        continue
                    try:
                        event = json.loads(line)
                        event_type = event.get("event")
                        callback(event_type, event)
                    except Exception as ex:
                        print(f"Error parsing event line: {line}\nException: {ex}")
        except Exception as e:
            print(f"Error starting run monitoring: {str(e)}")
