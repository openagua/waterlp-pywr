from datetime import datetime

from waterlp.pywr_model import NetworkModel
from waterlp.reporters.post_reporter import Reporter as PostReporter
from waterlp.reporters.ably_reporter import AblyReporter
from waterlp.screen_reporter import ScreenReporter

current_step = 0
total_steps = 0


def run_scenario(supersubscenario, args, verbose=False, **kwargs):

    global current_step, total_steps

    system = supersubscenario.get('system')

    # setup the reporter (ably is on a per-process basis)
    post_reporter = PostReporter(args) if args.post_url else None
    reporter = None
    if args.message_protocol is None:
        reporter = ScreenReporter(args)
    elif args.message_protocol == 'post':
        post_reporter.is_main_reporter = True
        reporter = post_reporter
    elif args.message_protocol == 'ably':  # i.e. www.ably.io
        ably_auth_url = args.ably_auth_url if 'ably_auth_url' in args else kwargs.pop('ably_auth_url', None)
        reporter = AblyReporter(args, ably_auth_url=ably_auth_url, post_reporter=post_reporter)

    if reporter:
        reporter.updater = system.scenario.update_payload
        system.scenario.reporter = reporter

    if post_reporter:
        post_reporter.updater = system.scenario.update_payload

    try:

        # for result in _run_scenario(system, args, conn, supersubscenario, reporter=reporter, verbose=verbose):
        #     pass
        _run_scenario(system, args, supersubscenario, reporter=reporter, verbose=verbose)

    except Exception as err:

        print(err)

        if reporter:
            reporter.report(action='error', message=str(err))


def _run_scenario(system=None, args=None, supersubscenario=None, reporter=None, verbose=False):
    global current_step, total_steps

    debug = args.debug

    # initialize with scenario
    # current_dates = system.dates[0:foresight_periods]

    # intialize
    system.initialize(supersubscenario)

    system.model = NetworkModel(
        network=system.network,
        template=system.template,
        solver=args.solver,
        # evaluator=system.evaluator,
    )

    total_steps = len(system.dates)

    runs = range(system.nruns)
    n = len(runs)

    i = 0
    now = datetime.now()

    while i < n:

        ts = runs[i]
        current_step = i + 1

        if verbose:
            print('current step: %s' % current_step)

        #######################
        # CORE SCENARIO ROUTINE
        #######################

        current_dates = system.dates[ts:ts + system.foresight_periods]
        current_dates_as_string = system.dates_as_string[ts:ts + system.foresight_periods]
        if ts != runs[-1]:
            step = (system.dates[ts + 1] - system.dates[ts]).days
        # 1. Update timesteps
        system.model.update_timesteps(
            start=current_dates_as_string[0],
            end=current_dates_as_string[-1],
            step=step
        )

        try:
            system.update_initial_conditions(
                variables=system.variables,
                initialize=i == 0
            )
            # system.update_boundary_conditions(ts, ts + system.foresight_periods, 'intermediary')
            # system.update_boundary_conditions(ts, ts + system.foresight_periods, 'model')
            system.update_boundary_conditions(ts, ts + system.foresight_periods)
            system.model.run()
            system.collect_results(current_dates_as_string, tsidx=i, suppress_input=args.suppress_input)

            # REPORT PROGRESS
            system.scenario.finished += 1
            system.scenario.current_date = current_dates_as_string[0]

            new_now = datetime.now()
            should_report_progress = ts == 0 or current_step == n or (new_now - now).seconds >= 2
            # system.dates[ts].month != system.dates[ts - 1].month and (new_now - now).seconds >= 1

            if system.scenario.reporter and should_report_progress:
                system.scenario.reporter.report(action='step')

                now = new_now

        except Exception as err:
            log_dir = system.save_logs()
            system.save_results(error=True)
            msg = 'ERROR: Something went wrong at step {timestep} of {total} ({date}):\n\n{err}'.format(
                timestep=current_step,
                total=total_steps,
                date=current_dates[0].date(),
                err=err
            )
            if log_dir:
                msg += '\n\nSee log files in "{}"'.format(log_dir)
            print(msg)
            if system.scenario.reporter:
                system.scenario.reporter.report(action='error', message=msg)

            raise Exception(msg)

        if ts == runs[-1]:
            system.save_results()
            reporter and reporter.report(action='done')

            print('finished')

        i += 1

        # yield

    # POSTPROCESSING HERE (IF ANY)

    # reporter.done(current_step, total_steps
