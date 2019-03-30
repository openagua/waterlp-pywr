import json

from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNOperationType, PNStatusCategory

from waterlp.reporters.redis import local_redis

class ProcessState:
    REQUESTED = b'requested'
    STARTED = b'started'
    RUNNING = b'running'
    PAUSED = b'paused'
    CANCELED = b'stopped'
    ERROR = b'error'
    FINISHED = b'finished'


def message_handler(message):
    if not message:
        return

    data = json.loads(message['data'])
    action = data.get('action')
    sid = data.get('sid')
    if sid and action == 'cancel':
        if local_redis:
            local_redis.set(sid, ProcessState.CANCELED)
            local_redis.expire(sid, 3600 * 24)


class PNSubscribeCallback(SubscribeCallback):
    def status(self, pubnub, status):
        pass
        # The status object returned is always related to subscribe but could contain
        # information about subscribe, heartbeat, or errors
        # use the operationType to switch on different options
        if status.operation == PNOperationType.PNSubscribeOperation \
                or status.operation == PNOperationType.PNUnsubscribeOperation:
            if status.category == PNStatusCategory.PNConnectedCategory:
                pass
                # This is expected for a subscribe, this means there is no error or issue whatsoever
            elif status.category == PNStatusCategory.PNReconnectedCategory:
                pass
                # This usually occurs if subscribe temporarily fails but reconnects. This means
                # there was an error but there is no longer any issue
            elif status.category == PNStatusCategory.PNDisconnectedCategory:
                pass
                # This is the expected category for an unsubscribe. This means there
                # was no error in unsubscribing from everything
            elif status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
                pass
                # This is usually an issue with the internet connection, this is an error, handle
                # appropriately retry will be called automatically
            elif status.category == PNStatusCategory.PNAccessDeniedCategory:
                pass
                # This means that PAM does allow this client to subscribe to this
                # channel and channel group configuration. This is another explicit error
            else:
                pass
                # This is usually an issue with the internet connection, this is an error, handle appropriately
                # retry will be called automatically
        elif status.operation == PNOperationType.PNSubscribeOperation:
            # Heartbeat operations can in fact have errors, so it is important to check first for an error.
            # For more information on how to configure heartbeat notifications through the status
            # PNObjectEventListener callback, consult <link to the PNCONFIGURATION heartbeart config>
            if status.is_error():
                pass
                # There was an error with the heartbeat operation, handle here
            else:
                pass
                # Heartbeat operation was successful
        else:
            pass
            # Encountered unknown status type

    def presence(self, pubnub, presence):
        pass  # handle incoming presence data

    def message(self, pubnub, data):
        message = data.message
        if message:
            state = message.get('state')
            sid = data.message.get('sid')
            if sid:
                local_redis.set(sid, state)
                local_redis.expire(sid, 3600 * 24)
                print(sid, state)

        return
