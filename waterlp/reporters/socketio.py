from os import environ
# from flask_socketio import SocketIO
from socketio import Client as SocketIOClient

def on_publish(envelope, status):
    # Check whether request successfully completed or not
    if not status.is_error():
        pass  # Message successfully published to specified channel.
    else:
        print("Failed to report progress.")
        pass  # Handle message publish error. Check 'category' property to find out possible issue
        # because of which request did fail.
        # Request can be resent using: [status retry];


class SocketIOReporter(object):

    def __init__(self, args, publish_key=None, post_reporter=None):
        self.args = args
        self.post_reporter = post_reporter
        self.updater = None
        self.channel = None

        subscribe_key = environ.get('SOCKETIO_SUBSCRIBE_KEY')

        # if publish_key and subscribe_key:

        model_key = environ.get('MODEL_KEY')
        hostname = environ.get('OPENAGUA_HOST')

        socketio_url = 'amqp://{username}:{password}@{hostname}/{vhost}/'.format(
            username=model_key,
            password="password",
            hostname=hostname,
            vhost=environ.get('OPENAGUA_VHOST', 'model-run')
        )
        self.socketio = SocketIOClient()

        self.room = 'source-{}-network-{}'.format(args.source_id, args.network_id)

        # self.channel = 'openagua-{source_id}-{network_id}-{model_key}'.format(
        #     source_id=args.source_id,
        #     network_id=args.network_id,
        #     model_key=model_key
        # )
        # if environ.get('RUN_KEY'):
        #     self.channel += '-{}'.format(environ['RUN_KEY'])
        # else:
        #     self.pubnub = None
        #     self.channel = None

    # publish updates
    def report(self, action, **payload):
        if self.updater:
            payload = self.updater(action=action, **payload)
        if action in ['step', 'save']:
            if self.socketio:
                # self.pubnub.publish().channel(self.channel).message(payload).pn_async(on_publish)
                self.socketio.emit("update-network-progress", payload, room=self.room)

            # elif self.post_reporter:
            #     self.post_reporter.report(**payload)
        else:
            if self.post_reporter:
                self.post_reporter.report(**payload)
            return

        if action in ['done', 'error']:
            return
