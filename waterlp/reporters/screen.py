# import progressbar


class ScreenReporter(object):

    def __init__(self, args):
        self.args = args
        self.updater = None
        self.progress = -999
        # self.bar = progressbar.ProgressBar(maxval=100,
        #                                    widgets=[progressbar.Bar('=', '[', ']'), ' ', progressbar.Percentage()])
        # self.bar.start()

    # publish updates
    def report(self, action, **payload):
        if self.updater:
            payload = self.updater(action=action, **payload)

        msg = ''

        if action == 'step':
            progress = payload.get('progress')
            if progress != self.progress:
                # self.bar.update(progress)
                msg = '{status} - progress: {progress}%'.format(**payload)
                print(msg)
            self.progress = progress
