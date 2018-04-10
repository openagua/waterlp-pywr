import logging

def create_logger(appname, logfile, msg_format):
    logger = logging.getLogger(appname)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter(msg_format)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
