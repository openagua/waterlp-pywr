#!/usr/bin/env python3

import os
import shutil
import sys
import getpass

from waterlp.tasks import run_model
from waterlp.parser import commandline_parser

if __name__ == '__main__':
    try:
        parser = commandline_parser()
        args, unknown = parser.parse_known_args(sys.argv[1:])

        if not hasattr(args, 'aws_s3_bucket'):
            args.aws_s3_bucket = os.environ.get('AWS_S3_BUCKET')

        app_dir = '/home/{}/.waterlp'.format(getpass.getuser())
        if os.path.exists(app_dir):
            shutil.rmtree(app_dir)
        logs_dir = '{}/logs'.format(app_dir)

        run_model(args, logs_dir)
    except Exception as e:
        print(e, file=sys.stderr)
