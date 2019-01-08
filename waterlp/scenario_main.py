import traceback
from io import StringIO

from pyomo.opt import SolverFactory, SolverStatus, TerminationCondition

from waterlp.pywr_model import create_model
from waterlp.post_reporter import Reporter as PostReporter
from waterlp.ably_reporter import AblyReporter
from waterlp.screen_reporter import ScreenReporter

current_step = 0
total_steps = 0


def run_scenario(supersubscenario, args=None, verbose=False):
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
        reporter = AblyReporter(args, post_reporter=post_reporter)

    if reporter:
        reporter.updater = system.scenario.update_payload
        system.scenario.reporter = reporter

    if post_reporter:
        post_reporter.updater = system.scenario.update_payload

    try:

        # for result in _run_scenario(system, args, conn, supersubscenario, reporter=reporter, verbose=verbose):
        #     pass
        _run_scenario(system, args, supersubscenario, reporter=reporter, verbose=verbose)

    except Exception as e:

        # print(e, file=sys.stderr)
        # Exception logging inspired by: https://seasonofcode.com/posts/python-multiprocessing-and-exceptions.html
        exc_buffer = StringIO()
        traceback.print_exc(file=exc_buffer)
        msg = 'At step ' + str(current_step) + ' of ' + str(total_steps) + ': ' + \
              str(e) + '\nUncaught exception in worker process:\n' + exc_buffer.getvalue()
        if current_step:
            msg += '\n\nPartial results have been saved'

        print(msg)

        if reporter:
            reporter.report(action='error', message=msg)


def _run_scenario(system=None, args=None, supersubscenario=None, reporter=None, verbose=False):
    global current_step, total_steps

    debug = args.debug

    # initialize with scenario
    # current_dates = system.dates[0:foresight_periods]

    # intialize
    system.initialize(supersubscenario)

    system.model = create_model(
        network=system.network,
        template=system.template,
        start=system.scenario.start_time,
        end=system.scenario.end_time,
        ts=system.scenario.time_step,
        params=system.params,
        debug_gain=args.debug_gain,
        debug_loss=args.debug_loss
    )

    system.instance = system.model.create_instance()

    system.update_internal_params()

    optimizer = SolverFactory(args.solver)

    total_steps = len(system.dates)

    runs = range(system.nruns)
    n = len(runs)

    i = 0
    while i < n:

        ts = runs[i]
        current_step = i + 1

        if verbose:
            print('current step: %s' % current_step)

        # if user requested to stop
        # if reporter._is_canceled:
        # print('canceled')
        # break

        #######################
        # CORE SCENARIO ROUTINE
        #######################

        current_dates = system.dates[ts:ts + system.foresight_periods]
        current_dates_as_string = system.dates_as_string[ts:ts + system.foresight_periods]

        # solve the model
        try:
            results = optimizer.solve(system.instance)
        except:
            system.save_results()
            msg = 'ERROR: Unknown error at step {} of {} ({}). Partial results have been saved.'.format(
                current_step, total_steps, current_dates[0]
            )
            if system.scenario.reporter:
                system.scenario.reporter.report(action='error', message=msg)
            raise Exception(msg)

        if (results.solver.status == SolverStatus.ok) \
                and (results.solver.termination_condition == TerminationCondition.optimal):
            system.collect_results(current_dates_as_string, tsidx=i, suppress_input=args.suppress_input)

        elif results.solver.termination_condition == TerminationCondition.infeasible:
            system.save_results()
            msg = 'ERROR: Problem is infeasible at step {} of {} ({}). Partial results have been saved.'.format(
                current_step, total_steps, current_dates[0]
            )
            if system.scenario.reporter:
                system.scenario.reporter.report(action='error', message=msg)
            raise Exception(msg)

        else:
            system.save_results()
            msg = 'ERROR: Something went wrong at step {} of {} ({}). This might indicate an infeasibility, but not necessarily.'.format(
                current_step, total_steps, current_dates[0]
            )
            print(msg)
            if system.scenario.reporter:
                system.scenario.reporter.report(action='error', message=msg)
            raise Exception(msg)

        # load the results
        system.instance.solutions.load_from(results)

        system.scenario.finished += 1
        system.scenario.current_date = current_dates_as_string[0]

        if system.scenario.reporter:
            system.scenario.reporter.report(action='step')

        # update the model instance
        if ts != runs[-1]:
            ts_next = runs[i + 1]
            try:
                system.update_initial_conditions()
                system.update_boundary_conditions(ts, ts + system.foresight_periods, 'intermediary')
                system.update_boundary_conditions(ts_next, ts_next + system.foresight_periods, 'model')
                system.update_internal_params()  # update internal parameters that depend on user-defined variables
            except:
                # we can still save results to-date
                system.save_results()
                msg = 'ERROR: Something went wrong at step {} of {} ({}). There is something wrong with the model. Results to-date have been saved'.format(
                    current_step, total_steps, current_dates[0]
                )
                print(msg)
                if system.scenario.reporter:
                    system.scenario.reporter.report(action='error', message=msg)

                raise Exception(msg)
            system.instance.preprocess()

        else:
            system.save_results()
            reporter and reporter.report(action='done')

            if verbose:
                print('finished')

        i += 1

        # yield

    # POSTPROCESSING HERE (IF ANY)

    # reporter.done(current_step, total_steps
