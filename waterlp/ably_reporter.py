from ably import AblyRest

class AblyReporter(object):

    def __init__(self, args, post_reporter, ably_auth_url=None):
        self.args = args
        self.post_reporter = post_reporter
        channel_name = u'com.openagua.update_s{}n{}'.format(args.source_id, args.network_id)
        if ably_auth_url:
            logging.getLogger('ably').setLevel(logging.CRITICAL)
            logger = logging.getLogger('ably')
            logger.addHandler(logging.StreamHandler())
            client_id = args.hydra_username
            model_secret = environ.get('MODEL_SECRET')
            rest = AblyRest(auth_url=ably_auth_url, auth_params={'client_id': client_id, 'model_secret': model_secret})
        # elif ably_token_request:
        #     rest = AblyRest(token=ably_token_request)
        else:
            rest = AblyRest(key=environ.get('ABLY_API_KEY'))
        self.channel = rest.channels.get(channel_name)
        self.updater = None

    # subscribe to actions
    def on_action(self, msg):
        action = msg['action']
        if action == 'stopall' or msg['action'] == 'stop' and msg['sid'] == args.unique_id:
            self.report(action='stop', progress=current_step / total_steps * 100)
        elif action == 'pause':
            paused = True
            self.report(action='pause', progress=current_step / total_steps * 100)
        elif action == 'resume':
            paused = False
            self.report(action='resume')

    # publish updates
    def report(self, action, **payload):
        if self.updater:
            payload = self.updater(action=action, **payload)
        if action in ['step', 'save']:
            self.channel.publish(action, payload)
        else:
            if self.post_reporter:
                self.post_reporter.report(**payload)
            return

        if action in ['done', 'error']:
            return
