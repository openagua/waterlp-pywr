from os import environ

from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub


def on_publish(envelope, status):
    # Check whether request successfully completed or not
    if not status.is_error():
        pass  # Message successfully published to specified channel.
    else:
        print("Failed to report progress.")
        pass  # Handle message publish error. Check 'category' property to find out possible issue
        # because of which request did fail.
        # Request can be resent using: [status retry];

class PubNubReporter(object):

    def __init__(self, args, publish_key=None, post_reporter=None):
        self.args = args
        self.post_reporter = post_reporter
        self.updater = None

        subscribe_key = environ.get('PUBNUB_SUBSCRIBE_KEY')

        if publish_key and subscribe_key:
            pnconfig = PNConfiguration()
            pnconfig.subscribe_key = subscribe_key
            pnconfig.publish_key = publish_key
            pnconfig.ssl = False
            self.pubnub = PubNub(pnconfig)
            self.channel = 'openagua-{source_id}-{network_id}-{model_key}'.format(
                source_id=args.source_id,
                network_id=args.network_id,
                model_key=environ['MODEL_KEY']
            )
            if environ.get('RUN_KEY'):
                self.channel += '-{}'.format(environ['RUN_KEY'])
            # print('Channel name: ' + self.channel)
        else:
            self.pubnub = None
            self.channel = None

    # publish updates
    def report(self, action, **payload):
        if self.updater:
            payload = self.updater(action=action, **payload)
        if action in ['step', 'save']:
            if self.pubnub:
                self.pubnub.publish().channel(self.channel).message(payload).pn_async(on_publish)
            # elif self.post_reporter:
            #     self.post_reporter.report(**payload)
        else:
            if self.post_reporter:
                self.post_reporter.report(**payload)
            return

        if action in ['done', 'error']:
            return
