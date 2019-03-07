#!/usr/bin/env python3

import argparse
from ast import literal_eval
import os
import shutil
import sys
import uuid
import getpass
from datetime import datetime

from waterlp.logger import create_logger
from waterlp.tasks import run_scenarios


def commandline_parser():
    """
        Parse the arguments passed in from the command line.
    """
    parser = argparse.ArgumentParser(
        description="""A water system optimization model framework for OpenAgua.
                    Written by David Rheinheimer <drheinheimer@itesm.mx>
                    (c) Copyright 2016, Tecnologico de Monterrey.
        """, epilog="For more information visit www.openagua.org",
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('--app', dest='app_name', help='''Name of the app.''')
    parser.add_argument('--durl', dest='data_url', help='''The Hydra Server URL.''')
    parser.add_argument('--f', dest='filename', help='''The name of the input JSON file if running locally.''')
    parser.add_argument('--user', dest='hydra_username',
                        default=os.environ.get('HYDRA_USERNAME'),
                        help='''The username for logging in to Hydra Server.''')
    parser.add_argument('--pw', dest='hydra_password',
                        default=os.environ.get('HYDRA_PASSWORD'),
                        help='''The password for logging in to Hydra Server.''')
    parser.add_argument('--sid', dest='session_id',
                        help='''The Hydra Server session ID.''')
    parser.add_argument('--uid', dest='user_id', type=int,
                        help='''The Hydra Server user_id.''')
    parser.add_argument('--src', dest='source_id', type=int,
                        help='''The source ID of the model to be run.''')
    parser.add_argument('--nid', dest='network_id', type=int,
                        help='''The network ID of the model to be run.''')
    parser.add_argument('--tid', dest='template_id', type=int,
                        help='''The template ID of the model to be run.''')
    parser.add_argument('--scids', dest='scenario_ids',
                        help='''The IDs of the scenarios to be run,
                        specified as a string containing a comma-separated list of
                        integers or integer tuples.
                        ''')
    parser.add_argument('--htsf', dest='hydra_timestep_format',
                        help='''The format of a time step in Hydra Platform (found in hydra.ini).''')
    parser.add_argument('--ldir', dest='log_dir',
                        help='''The main log file directory.''')
    parser.add_argument('--rname', dest='run_name', help='''Name of the run. This will be added to result scenarios.''')
    parser.add_argument('--sol', dest='solver', default='glpk',
                        help='''The solver to use (e.g., glpk, gurobi, etc.).''')
    parser.add_argument('--fs', dest='foresight', default='zero', help='''Foresight: 'perfect' or 'imperfect' ''')
    parser.add_argument('--purl', dest='post_url',
                        help='''URL to ping indicating activity.''')
    parser.add_argument('--mp', dest='message_protocol', default=None,
                        help='''Message protocol to report progress back to client browser''')
    parser.add_argument('--wurl', dest='websocket_url',
                        help='''URL and port that is listening for activity.''')
    parser.add_argument('--guid', default=uuid.uuid4().hex, dest='unique_id',
                        help='''Unique identifier for this run.''')
    parser.add_argument('--debug', dest='debug', action='store_true', help='''Debug flag.''')
    parser.add_argument('--debug_ts', dest='debug_ts', type=int, default=10,
                        help='''The number of timesteps to run in debug mode.''')
    parser.add_argument('--debug_gain', dest='debug_gain', action='store_true',
                        help='''Debug flag for the Pyomo model.''')
    parser.add_argument('--debug_loss', dest='debug_loss', action='store_true',
                        help='''Debug flag for the Pyomo model.''')
    parser.add_argument('--c', dest='custom', type=dict, default={},
                        help='''Custom arguments passed as stringified JSON.''')
    parser.add_argument('--dest', dest='destination', default='source',
                        help='''Destination of results. Options for now include "source" or "aws_s3"''')
    parser.add_argument('--si', dest='suppress_input', action='store_true',
                        help='''Suppress input from results. This can speed up writing results.''')
    parser.add_argument('--st', dest='start_time', default=datetime.now().isoformat(), help='''Run start time.''')

    return parser


def run_model(args, logs_dir, **kwargs):
    # initialize log directories
    args.log_dir = os.path.join(logs_dir, args.log_dir)

    # specify scenarios log dir
    args.scenario_log_dir = 'scenario_logs'
    args.scenario_log_dir = os.path.join(args.log_dir, args.scenario_log_dir)

    if not os.path.exists(args.log_dir):
        os.makedirs(args.log_dir)
    if not os.path.exists(args.scenario_log_dir):
        os.makedirs(args.scenario_log_dir)

    # create top-level log file
    logfile = os.path.join(args.log_dir, 'log.txt')
    networklog = create_logger(args.app_name, logfile, '%(asctime)s - %(message)s')

    # pre-processing
    if args.scenario_ids:
        args.scenario_ids = literal_eval(args.scenario_ids)

    argdict = args.__dict__.copy()
    argtuples = sorted(argdict.items())
    args_str = '\n\t'.join([''] + ['{}: {}'.format(a[0], a[1]) for a in argtuples])
    networklog.info('started model run with args: %s' % args_str)

    for key in kwargs:
        args.__setattr__(key, kwargs.get(key))

    run_scenarios(args, networklog)

    return


if __name__ == '__main__':
    try:
        parser = commandline_parser()
        args, unknown = parser.parse_known_args(sys.argv[1:])

        app_dir = '/home/{}/.waterlp'.format(getpass.getuser())
        if os.path.exists(app_dir):
            shutil.rmtree(app_dir)
        logs_dir = '{}/logs'.format(app_dir)
        run_model(args, logs_dir)
    except Exception as e:
        print(e, file=sys.stderr)
