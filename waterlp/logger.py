from os import path
import logging
from datetime import datetime


def create_logger(appname, logfile, msg_format):
    logger = logging.getLogger(appname)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(msg_format)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


class RunLogger(object):
    def __init__(self, name, app_name, run_name, logs_dir, username):
        self.app_name = app_name
        self.username = username
        self.run_name = run_name

        self.log = create_logger(name, path.join(logs_dir, 'log.txt'), '%(asctime)s - %(message)s')

    def message(self, msg):
        return '{app_name} / {username} {run_name} - {msg}'.format(
            app_name=self.app_name,
            username=self.username,
            run_name=self.run_name,
            msg=msg
        )

    def log_start(self):
        message = self.message('Started at UTC {start_time}'.format(
            app_name=self.app_name,
            start_time=datetime.utcnow(),
            user_name=self.username
        ))
        self.log.info(msg=message)

    def log_finish(self):
        message = self.message('Ended at UTC {end_time}'.format(
            app_name=self.app_name,
            end_time=datetime.utcnow(),
        ))
        self.log.info(msg=message)

    def log_error(self, message):
        self.log.info(msg=message)
