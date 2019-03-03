from datetime import datetime

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

    # 1. UPDATE INITIAL CONDITIONS
    # TODO: delete this once the irregular time step routine of Pywr is implemented

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
        step = (system.dates[ts] - system.dates[ts - 1]).days if ts else system.dates[0].day
        # 1. Update timesteps
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
