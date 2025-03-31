import threading

from entities_common import UtilsInterface


logging_utility = UtilsInterface.LoggingUtility()


class MonitorLauncher:
    def __init__(self, client, actions_client, run_id, callback_override=None, events=None):
        self.client = client
        self.actions_client = actions_client
        self.run_id = run_id
        self.callback_override = callback_override

        self.events=events

        def log_status_change(run_id, new_status, old_status, run_data):
            logging_utility.info(f"[MONITOR STATUS] {run_id}: {old_status} -> {new_status}")

        def log_completion(run_id, final_status, run_data):
            logging_utility.info(f"[MONITOR COMPLETE] {run_id} ended with status: {final_status}")

        def log_error(run_id, error_msg):
            logging_utility.error(f"[MONITOR ERROR] {run_id}: {error_msg}")


        self.monitor = self.events.HttpRunMonitor(
            run_id=run_id,
            runs_client=client.runs,
            actions_client=actions_client,
            on_status_change=log_status_change,
            on_complete=log_completion,
            on_error=log_error,
            on_action_required=self._default_callback
        )

    def _default_callback(self, run_id, run_data, pending_actions):
        try:
            logging_utility.info(f"[ACTION_REQUIRED] run {run_id} has {len(pending_actions)} pending action(s)")
            for action in pending_actions:
                tool = action.get('tool_name')
                args = action.get('function_args')
                logging_utility.info(f"[ACTION] Tool: {tool}, Args: {args}")
        except Exception as e:
            logging_utility.error(f"[MonitorLauncher] Error fetching actions: {e}")

    def _monitor_loop(self):
        try:
            logging_utility.info(f"[MonitorLauncher] Starting monitor for run {self.run_id}")
            self.monitor.start()
        except Exception as e:
            logging_utility.error(f"[MonitorLauncher] Monitoring thread failed: {e}")

    def start(self):
        t = threading.Thread(target=self._monitor_loop)
        t.daemon = True
        t.start()
        logging_utility.info(f"[MonitorLauncher] Launched thread for run {self.run_id}")
