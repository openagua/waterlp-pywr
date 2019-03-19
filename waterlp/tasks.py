import os
import getpass
import json
from datetime import datetime
from itertools import product
from copy import copy
from ast import literal_eval
from os import environ

from waterlp.celery_app import app
from celery.exceptions import Ignore

from waterlp.reporters.redis import local_redis
from waterlp.reporters.post import Reporter as PostReporter
from waterlp.reporters.ably import AblyReporter
from waterlp.reporters.pubnub import PubNubReporter
from waterlp.reporters.screen import ScreenReporter
from waterlp.logger import RunLogger
from waterlp.parser import commandline_parser
from waterlp.connection import connection
from waterlp.logger import create_logger
from waterlp.models.system import WaterSystem
from waterlp.scenario_class import Scenario
from waterlp.utils.scenarios import create_subscenarios
from waterlp.utils.application import ProcessState

current_step = 0
total_steps = 0


class Object(object):
    def __init__(self, values):
        for key in values:
            setattr(self, key, values[key])


@app.task(name='openagua.run')
def run(**kwargs):
    """This is for starting the model with Celery"""
    env = kwargs.get('env', {})
    args = kwargs.get('args')
    kwargs = kwargs.get('kwargs')

    # parse arguments
    parser = commandline_parser()
    args, unknown = parser.parse_known_args(args)

    # specify the log directory
    app_dir = '/home/{}/.waterlp'.format(getpass.getuser())
    logs_dir = '{}/logs'.format(app_dir)

    # set up some environment variables
    # TODO: can probably pass these directly, since we now have them...
    for key, value in env.items():
        os.environ[key] = value
    for key in kwargs:
        setattr(args, key, kwargs[key])
    print(' [x] Running "{}" with {}'.format(args.run_name, args))

    RunLog = RunLogger(name='waterlp', app_name=args.app_name, run_name=args.run_name, logs_dir=logs_dir,
                       username=args.hydra_username)

    try:
        RunLog.log_start()
        run_model(args, logs_dir, **kwargs)
        RunLog.log_finish()
    except:
        pass


def run_model(args, logs_dir, **kwargs):
    # initialize log directories
    if not args.log_dir:
        args.log_dir = 'network-{}'.format(args.network_id)
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
    networklog.info('Started model run with args: %s' % args_str)

    for key in kwargs:
        setattr(args, key, kwargs.get(key))

    run_scenarios(args, networklog)

    return


def run_scenarios(args, networklog):
    """
        This is a wrapper for running all the scenarios, where scenario runs are
        processor-independent. As much of the model is created here as
        possible, to be efficient in setup processing.
    """

    verbose = False

    print('')
    if args.debug:
        print("DEBUG ON")
    else:
        print("DEBUG OFF")

    args.starttime = datetime.now()  # args.start_time is iso-formatted, but this is still probably redundant

    if args.debug:
        print("================================================")
        print("STARTING RUN")
        print("Start time: {}".format(args.starttime.isoformat()))
        print("================================================")

    # ======================
    # connect to data server
    # ======================
    all_scenario_ids = list(set(sum(args.scenario_ids, ())))

    conn = connection(args=args, scenario_ids=all_scenario_ids)

    # ====================================
    # define subscenarios (aka variations)
    # ====================================

    # this gets all scenarios in the system, not just the main scenarios of interest, but without data
    network = conn.get_basic_network()

    # create the system
    base_system = WaterSystem(
        conn=conn,
        name=args.app_name,
        all_scenarios=network.scenarios,
        network=conn.network,
        template=conn.template,
        date_format='%Y-%m-%d %H:%M:%S',
        args=args,
    )

    all_supersubscenarios = []

    # prepare the reporter
    post_reporter = PostReporter(args) if args.post_url else None

    for scenario_ids in args.scenario_ids:

        try:
            scenario_ids = list(scenario_ids)
        except:
            scenario_ids = [scenario_ids]

        sid = '-'.join([args.unique_id] + [str(s) for s in set(scenario_ids)])

        try:
            if local_redis.get(sid) == ProcessState.CANCELED:
                print('Canceled by user')
                raise Ignore
        except Exception as err:
            print(err)
            raise

        # create the scenario class
        scenario = Scenario(scenario_ids=scenario_ids, conn=conn, network=conn.network, args=args)

        start_payload = scenario.update_payload(action='start')
        networklog.info(msg="Model started")
        if post_reporter:
            post_reporter.start(is_main_reporter=(args.message_protocol == 'post'), **start_payload)

        # create the system class
        # TODO: pass resources as dictionaries instead for quicker lookup
        option_subscenarios = create_subscenarios(conn.network, conn.template, scenario.option, 'option')
        scenario_subscenarios = create_subscenarios(conn.network, conn.template, scenario.scenario, 'scenario')

        try:

            # prepare the system
            system = copy(base_system)
            system.scenario = scenario
            system.initialize_time_steps()
            system.collect_source_data()

            # organize the subscenarios
            flattened = product(option_subscenarios, scenario_subscenarios)
            subscenario_count = len(option_subscenarios) * len(scenario_subscenarios)

            if args.debug:
                verbose = True
                system.nruns = min(args.debug_ts, system.nruns)
                system.dates = system.dates[:system.nruns]
                system.dates_as_string = system.dates_as_string[:system.nruns]

                subscenario_count = min(subscenario_count, 1)

            system.scenario.subscenario_count = subscenario_count
            system.scenario.total_steps = subscenario_count * len(system.dates)

            supersubscenarios = [{
                'i': i + 1,
                'sid': sid,
                'system': copy(system),  # this is intended to be a shallow copy
                'variation_sets': variation_sets,
            } for i, variation_sets in enumerate(flattened)]

            all_supersubscenarios.extend(supersubscenarios[:subscenario_count])

        except Exception as err:
            err_class = err.__class__.__name__
            if err_class == 'InnerSyntaxError':
                m = err.message
            else:
                # m = "Unknown error."
                m = str(err)
            message = "Error: Failed to prepare system.\n\n{}".format(m)
            print(message)
            if post_reporter:
                payload = scenario.update_payload(action='error', message=message)
                post_reporter.report(**payload)
            else:
                print(message)

            networklog.info(msg=message)

            raise

    # ================
    # run the scenario
    # ================

    if args.debug:
        run_scenario(all_supersubscenarios[0], args=args, verbose=verbose)
    else:
        for ss in all_supersubscenarios:
            run_scenario.apply_async((ss, args, verbose), serializer='pickle', compression='gzip')
    return


@app.task
def run_scenario(supersubscenario, args, verbose=False):
    print("RUNNING SCENARIO: {}".format(print(supersubscenario)))

    global current_step, total_steps

    # Check OA to see if the model request is still valid
    sid = supersubscenario.get('sid')
    if local_redis.get(sid) == ProcessState.CANCELED:
        print("Canceled by user.")
        raise Ignore

    system = supersubscenario.get('system')

    # setup the reporter (ably is on a per-process basis)
    post_reporter = PostReporter(args) if args.post_url else None
    reporter = None
    if args.message_protocol is None:
        reporter = ScreenReporter(args)
    elif args.message_protocol == 'post':
        post_reporter.is_main_reporter = True
        reporter = post_reporter
    elif args.message_protocol == 'ably':
        reporter = AblyReporter(args, post_reporter=post_reporter)
    elif args.message_protocol == 'pubnub':
        reporter = PubNubReporter(args, publish_key=args.publish_key, post_reporter=post_reporter)
    if reporter:
        reporter.updater = system.scenario.update_payload
        system.scenario.reporter = reporter

    if post_reporter:
        post_reporter.updater = system.scenario.update_payload

    try:

        # for result in _run_scenario(system, args, conn, supersubscenario, reporter=reporter, verbose=verbose):
        #     pass
        _run_scenario(system, args, supersubscenario, reporter=reporter, verbose=verbose)

    except Ignore as err:
        raise

    except Exception as err:

        print(err)

        if reporter:
            reporter.report(action='error', message=str(err))


def _run_scenario(system=None, args=None, supersubscenario=None, reporter=None, verbose=False):
    global current_step, total_steps

    debug = args.debug

    sid = supersubscenario.get('sid')

    # initialize with scenario
    # current_dates = system.dates[0:foresight_periods]

    # intialize
    system.initialize(supersubscenario)

    # 1. UPDATE INITIAL CONDITIONS
    # TODO: delete this once the irregular time step routine of Pywr is implemented

    total_steps = len(system.dates)

    runs = range(system.nruns)
    n = len(runs)

    i = 0
    now = datetime.now()

    while i < n:

        if local_redis.get(sid) == ProcessState.CANCELED:
            print("Canceled by user.")
            raise Ignore

        ts = runs[i]
        current_step = i + 1

        # if verbose:
        #     print('current step: %s' % current_step)

        #######################
        # CORE SCENARIO ROUTINE
        #######################

        # 1. Update timesteps

        # TODO: update time step scheme based on https://github.com/pywr/pywr/issues/688
        current_dates = system.dates[ts:ts + system.foresight_periods]
        current_dates_as_string = system.dates_as_string[ts:ts + system.foresight_periods]
        step = (system.dates[ts] - system.dates[ts - 1]).days if ts else system.dates[0].day

        if system.scenario.time_step != 'day':

            system.model.update_timesteps(
                start=current_dates_as_string[0],
                end=current_dates_as_string[-1],
                step=step
            )

        try:

            # 2. UPDATE BOUNDARY CONDITIONS

            system.update_boundary_conditions(ts, ts + system.foresight_periods, step='pre-process')
            system.update_boundary_conditions(ts, ts + system.foresight_periods, step='main')

            # 3. RUN THE MODEL ONE TIME STEP

            results = system.step()

            if i == 0 and args.debug and results:
                stats = results.to_dataframe()
                content = stats.to_csv()
                system.save_to_file('stats.csv', content)

            # 4. COLLECT RESULTS
            system.collect_results(current_dates_as_string, tsidx=i, suppress_input=args.suppress_input)

            # 5. CALCULATE POST-PROCESSED RESULTS
            system.update_boundary_conditions(ts, ts + system.foresight_periods, step='post-process')

            # 6. REPORT PROGRESS
            system.scenario.finished += 1
            system.scenario.current_date = current_dates_as_string[0]

            new_now = datetime.now()
            should_report_progress = ts == 0 or current_step == n or (new_now - now).seconds >= 2
            # system.dates[ts].month != system.dates[ts - 1].month and (new_now - now).seconds >= 1

            if system.scenario.reporter and should_report_progress:
                system.scenario.reporter.report(action='step')

                now = new_now

        except Exception as err:
            saved = system.save_logs()
            system.save_results(error=True)
            msg = 'ERROR: Something went wrong at step {timestep} of {total} ({date}):\n\n{err}'.format(
                timestep=current_step,
                total=total_steps,
                date=current_dates[0].date(),
                err=err
            )
            if saved:
                msg += '\n\nSee log files in "{}"'.format(args.log_dir)
            print(msg)
            if system.scenario.reporter:
                system.scenario.reporter.report(action='error', message=msg)

            raise Exception(msg)

        if ts == runs[-1]:
            system.finish()
            reporter and reporter.report(action='done')

            print('finished')

        i += 1

        # yield

    # POSTPROCESSING HERE (IF ANY)

    # reporter.done(current_step, total_steps
