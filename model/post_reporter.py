## This file contains all the logic to report to OpenAgua via POST requests
import requests
import threading
import json
import time


class Reporter:
    """This reporter posts a message to the OpenAgua API. It is mostly just a passthrough, with payload being defined outside."""

    def __init__(self, args):

        self._post_url = args.post_url
        self._paused = False
        self._canceled = False
        self.updater = None
        self.is_main_reporter = False

    def send(self, **payload):
        action = payload.get('action')
        if self.is_main_reporter:
            payload = {**self.base_payload, **payload}
        return requests.post('{}/{}'.format(self._post_url, action), json=payload)

    def report(self, action=None, **payload):
        action = payload.get('action', action)
        if self.updater:
            payload = self.updater(action=action, **payload)
        if action == 'step':
            elapsed_time = round(time.time() - self.start_time)
            if elapsed_time % 2 == 0 and elapsed_time != self.old_elapsed_time or payload.get('progress') == 100:
                self.send(**payload)
            self.old_elapsed_time = elapsed_time

        if action == 'error':
            if self.is_main_reporter:
                self._cancel_heartbeat()
            payload['extra_info'] = payload.get('message')
        return self.send(**payload)

    def start(self, is_main_reporter=True, **payload):
        self.is_main_reporter = is_main_reporter
        if is_main_reporter:
            # self._init_heartbeat()
            self.start_time = time.time()
            self.old_elapsed_time = 0
            self.base_payload = payload
        self.report(**payload)

    def _init_heartbeat(self):
        self.heartbeat_timer = threading.Timer(5, self._heartbeat).start()
        return

    def _heartbeat(self):

        result = self.report(action='ping')

        ## we send back the client status with the ping status so we know
        ## if they want to stop/pause the process.
        res = json.loads(result.text)
        state = res.get('current_state', None)
        if state is not None:
            state = state.lower()

        if state == 'paused':
            self._paused = True
        if state == 'running':
            self._paused = False
        if state == 'stopped':
            self._canceled = True

        self._init_heartbeat()

    def _cancel_heartbeat(self):
        # if self.heartbeat_timer is not None:
        # self.heartbeat_timer.cancel()
        return
