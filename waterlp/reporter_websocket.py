## This file contains all the logic to report to OpenAgua
import asyncio
import json
import threading
import websockets


# import wingdbstub

class Reporter:
    def __init__(self, args):
        self.flavor = 'websocket'

        self._ws_url = args.websocket_url
        self._id = args.unique_id
        self._source_id = int(args.source_id)
        self._network_id = int(args.network_id)
        self._paused = False
        self._canceled = False
        self.heartbeat_timer = None
        self._start(args)

    def send_event(self, event, data={}):
        data.update(dict(
            sid=self._id,
            source_id=self._source_id,
            network_id=self._network_id
        ))
        asyncio.get_event_loop().run_until_complete(self._send_event(event, data))

    async def _send_event(self, event, data=None):
        async with websockets.connect(self._ws_url) as websocket:
            await websocket.send(json.dumps({'action': event, 'data': data}))

    async def send_ping(self, sid):
        async with websockets.connect(self._ws_url) as websocket:
            await websocket.send(json.dumps({'action': 'ping', 'data': sid}))
            # the server will send back the current state of the model so we know
            # if the user paused/resumed/stopped the run from the UI.
            return await websocket.recv()

    def ping(self, sid):
        ## since this is in it's own thread, we need to get a new event loop
        ## TODO this may be bad according to: https://stackoverflow.com/questions/46727787/runtimeerror-there-is-no-current-event-loop-in-thread-in-async-apscheduler
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        done = asyncio.get_event_loop().run_until_complete(self.send_ping(sid))
        loop.close()
        return done

    def _init_heartbeat(self):
        self.heartbeat_timer = threading.Timer(1, self._heartbeat).start()

    def _heartbeat(self):

        ## we send back the client status with the ping status so we know
        ## if they want to stop/pause the process.
        state = self.ping(self._id)

        if state is not None:
            state = state.lower()

        if state == 'paused':
            self._paused = True
        if state == 'running':
            self._paused = False
        if state == 'canceled':
            self._canceled = True

        self._init_heartbeat()

    def _cancel_heartbeat(self):
        if self.heartbeat_timer is not None:
            self.heartbeat_timer.cancel()

    def _start(self, args):
        self._init_heartbeat()
        self.send_event('start', data={'extra_info': str(args)})

    def is_paused(self):
        return self._paused

    def is_canceled(self):
        return self._canceled

    def step(self, current_step, total_steps):
        self.send_event('step', data={'extra_info': str(current_step) + '/' + str(total_steps)})

    def done(self, current_step, total_steps):
        self._cancel_heartbeat()
        if self.is_canceled():
            self.send_event('cancel', data={'extra_info': 'at step: ' + str(current_step) + '/' + str(total_steps)})
        else:
            self.send_event('done')

    def error(self, msg):
        self._cancel_heartbeat()
        self.send_event('error', data={'extra_info': msg})
