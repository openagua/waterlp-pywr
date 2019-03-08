from os import environ

from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub


class PubNubReporter(object, pub_key=None):

    def __init__(self, args, pub_key=None, post_reporter=None):
        self.args = args
        self.post_reporter = post_reporter
        self.updater = None

        sub_key = environ.get('PUBNUB_SUBSCRIBE_KEY')

        if pub_key and sub_key:
            pnconfig = PNConfiguration()
            pnconfig.subscribe_key = sub_key
            pnconfig.publish_key = pub_key
            pnconfig.ssl = False
            self.pubnub = PubNub(pnconfig)
            self.channel = u'com.openagua.update_s{}n{}'.format(args.source_id, args.network_id)
        else:
            self.pubnub = None
            self.channel = None

    # publish updates
    def report(self, action, **payload):
        if self.updater:
            payload = self.updater(action=action, **payload)
        if action in ['step', 'save']:
            if self.pubnub:
                self.pubnub.publish().channel(self.channel).message(action, payload)
            # elif self.post_reporter:
            #     self.post_reporter.report(**payload)
        else:
            if self.post_reporter:
                self.post_reporter.report(**payload)
            return

        if action in ['done', 'error']:
            return
