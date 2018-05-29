import hashlib
import json
import sys
import traceback
from copy import copy
from ast import literal_eval

import pandas as pd
import pendulum

myfuncs = {}


def get_scenarios_data(conn, scenario_ids, **kwargs):
    evaluator = Evaluator(conn, settings=kwargs['settings'], data_type=kwargs['data_type'])

    scenarios_data = []
    for i, scenario_id in enumerate(scenario_ids):
        evaluator.scenario_id = scenario_id
        scenario_data = get_scenario_data(evaluator, **kwargs)
        if scenario_data['data'] is None and i:
            scenario_data = copy(scenarios_data[i - 1])
            scenario_data.update({
                'id': scenario_id,
                'data': None,
                'error': 2
            })
        scenarios_data.append(scenario_data)

    return scenarios_data


def get_scenario_data(evaluator, **kwargs):
    kwargs['scenario_id'] = [evaluator.scenario_id]
    res_attr_data = evaluator.conn.get_res_attr_data(**kwargs)

    if res_attr_data and 'errorcode' not in res_attr_data:
        res_attr_data = res_attr_data[0]
        # evaluate the data
        # kwargs['data_type'] = res_attr_data.value.type

        eval_value = evaluator.eval_data(
            value=res_attr_data.value,
            do_eval=False
        )
        if eval_value is None:
            eval_value = evaluator.default_timeseries

        scenario_data = {
            'data': res_attr_data,
            'eval_value': eval_value,
            'error': 0
        }

    else:
        scenario_data = {'data': None, 'eval_value': evaluator.default_timeseries, 'error': 1}

    scenario_data['id'] = evaluator.scenario_id

    return scenario_data


def empty_data_timeseries(dates, date_format='iso', flavor='json'):
    values = [None] * len(dates)
    if flavor == 'json':
        if date_format == 'iso':
            timeseries = pd.DataFrame({'0': values}, index=dates).to_json(date_format='iso')
        elif date_format == 'original':
            timeseries = pd.DataFrame({'0': values}, index=dates, parse_dates=False)
    elif flavor == 'dict':
        timeseries = pd.DataFrame({0: values}, index=dates).to_dict()
    return timeseries


def eval_scalar(x):
    try:  # create the function
        if type(x) == str and len(x):
            x = float(x)
        else:
            x = None
    except ValueError as err:  # value error
        # err_class = err.__class__.__name__
        # detail = err.args[0]
        returncode = -1
        errormsg = "\"{}\" is not a number".format(x)
        result = None
    else:
        result = x
        returncode = 0
        errormsg = ''

    return returncode, errormsg, result


def eval_descriptor(s):
    result = s
    returncode = 0
    errormsg = 'No errors!'

    return returncode, errormsg, result


def eval_timeseries(timeseries, dates, date_format, fill_value=None, method=None, flavor=None):
    df = pd.read_json(timeseries)
    if df.empty:
        df = pd.DataFrame(index=dates, columns=['0'])
    else:
        # df = df.reindex(pd.DatetimeIndex(dates))
        if fill_value is not None:
            df.fillna(value=fill_value, inplace=True)
        elif method:
            df.fillna(method=method)

    if flavor is None or flavor == 'json':
        result = df.to_json(date_format=date_format)
    elif flavor == 'dict':
        df.index = df.index.strftime(date_format)
        result = df.to_dict()

    returncode = 0
    errormsg = 'No errors!'

    return returncode, errormsg, result


def eval_array(array):
    try:
        array = literal_eval(array)
        returncode = 0
        errormsg = 'No errors!'
    except:
        returncode = 1
        errormsg = 'Something is wrong.'

    return returncode, errormsg, array


def parse_function(s, name, modules=()):
    '''Parse a function into usable Python'''
    spaces = '\n    '

    # modules
    modules = spaces.join(modules)

    # first cut
    s = s.rstrip()
    lines = s.split('\n')
    if 'return ' not in lines[-1]:
        lines[-1] = 'return ' + lines[-1]
    code = spaces.join(lines)

    # final function
    func = '''def {name}(self, date, timestep, counter):{spaces}{modules}{spaces}{spaces}{code}''' \
        .format(spaces=spaces, modules=modules, code=code, name=name)

    return func


def make_dates(settings, date_format=None):
    # TODO: Make this more advanced - this should be pulled out into a different library available to all

    timestep = settings.get('time_step') or settings.get('timestep')
    start = settings.get('start_time') or settings.get('start')
    end = settings.get('end_time') or settings.get('end')

    dates = []
    if start and end:
        start = pendulum.parse(start)
        end = pendulum.parse(end)

        if timestep in ['day', 'week', 'month']:
            period = pendulum.period(start, end)
            dates = period.range("{}s".format(timestep))

        elif timestep == 'thricemonthly':
            period = pendulum.period(start, end)
            dates = []
            for dt in period.range('months'):
                d1 = pendulum.create(dt.year, dt.month, 10)
                d2 = pendulum.create(dt.year, dt.month, 20)
                d3 = dt.last_of('month')
                dates.extend([d1, d2, d3])

    if date_format is None:
        dates_as_string = [date.to_datetime_string() for date in dates]
    else:
        dates_as_string = [date.strftime(date_format) for date in dates]

    return dates_as_string, dates


def make_default_value(data_type, dates=None, flavor='json', date_format='iso'):
    if data_type == 'timeseries':
        default_eval_value = empty_data_timeseries(dates, flavor=flavor, date_format=date_format)
    elif data_type == 'array':
        default_eval_value = '[[],[]]'
    else:
        default_eval_value = ''
    return default_eval_value


class InnerSyntaxError(SyntaxError):
    """Exception for syntax errors that will be defined only where the SyntaxError is made.

    Attributes:
        expression -- input expression in which the error occurred
        message    -- explanation of the error
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class Evaluator:
    def __init__(self, conn=None, scenario_id=None, settings=None, date_format=None, data_type=None):
        self.conn = conn
        self.date_format = date_format
        self.dates_as_string, self.dates = make_dates(settings, date_format=date_format)
        self.scenario_id = scenario_id
        self.data_type = data_type
        self.default_timeseries = make_default_value('timeseries', self.dates_as_string, flavor='dict',
                                                     date_format='original')
        self.default_array = make_default_value('array')

        self.calculators = {}

        self.data = {}
        # This stores data that can be referenced later, in both time and space. The main purpose is to minimize repeated calls to data sources, especially when referencing other networks/resources/attributes within the project. While this needs to be recreated on every new evaluation or run, within each evaluation or run this can store as much as possible for reuse.

    def eval_data(self, value, func=None, do_eval=False, data_type=None, flavor=None, counter=0, fill_value=None,
                  res_attr_id=None):
        # create the data depending on data type

        returncode = None
        errormsg = None
        result = None

        # metadata = json.loads(resource_scenario.value.metadata)
        metadata = json.loads(value.metadata)
        if func is None:
            func = metadata.get('function')
        usefn = metadata.get('use_function', 'N')
        # if value is None:
        #     value = value.value
        data_type = value.type

        if usefn == 'Y':
            func = func if type(func) == str else ''
            try:
                returncode, errormsg, result = self.eval_function(func, flavor=flavor, counter=counter, uid=value['id'],
                                                                  res_attr_id=res_attr_id)
            except InnerSyntaxError:
                raise
            except Exception as e:
                print(e)
            if data_type == 'timeseries' and result is None:
                result = self.default_timeseries

        elif data_type == 'scalar':
            returncode, errormsg, result = eval_scalar(value.value)

        elif data_type == 'timeseries':

            returncode, errormsg, result = eval_timeseries(value.value, self.dates_as_string, self.date_format,
                                                           fill_value=fill_value, flavor=flavor)

        elif data_type == 'array':
            returncode, errormsg, result = eval_array(value.value)

        elif data_type == 'descriptor':
            returncode, errormsg, result = eval_descriptor(value.value)

        if do_eval:
            return returncode, errormsg, result
        else:
            return result

    def eval_function(self, code_string, counter=None, flavor=None, uid=None, res_attr_id=None):

        # assume there will be an exception:
        err_class = None
        line_number = None
        exception = True
        result = None
        detail = None
        value = None

        key = hashlib.sha224(str.encode(code_string)).hexdigest()

        # check if we already know about this function so we don't
        # have to do duplicate (possibly expensive) execs
        if key not in myfuncs:
            try:
                # create the string defining the wrapper function
                # Note: functions can't start with a number so pre-pend "func_"
                func_name = "func_{}".format(key)
                func = parse_function(code_string, name=func_name)
                # TODO : exec is unsafe
                exec(func, globals())
                exception = False
                myfuncs[key] = func_name
            except SyntaxError as err:  # syntax error
                line_number = err.lineno - 2
                res_attr_id = res_attr_id
                resource_name = self.conn.raid_to_res_name.get(res_attr_id, "unknown")
                errormsg = "Syntax error at line {}. (Resource: {})".format(err.lineno - 1, resource_name)
                raise InnerSyntaxError(expression=code_string, message=errormsg)
            except InnerSyntaxError:
                raise
            except Exception:
                raise

        try:
            # CORE EVALUATION ROUTINE
            values = []
            # dates = self.current_dates[self.tsi: self.tsf] # or self.dates_as_string
            for date in self.dates[self.tsi:self.tsf]:
                timestep = self.tsi + 1
                value = globals()[myfuncs[key]](self, date=date, timestep=timestep, counter=counter + 1)
                values.append(value)
                if self.data_type != 'timeseries':
                    break
            if self.data_type == 'timeseries':
                dates_idx = self.dates_as_string[self.tsi:self.tsf]
                if type(values[0]) in (list, tuple):
                    cols = range(len(values[0]))
                    if flavor is None:
                        result = pd.DataFrame.from_records(data=values, index=dates_idx, columns=cols).to_json(
                            date_format='iso')
                    elif flavor == 'pandas':
                        result = pd.DataFrame.from_records(data=values, index=dates_idx, columns=cols)
                    elif flavor == 'dict':
                        result = {c: {d: v[c] for d, v in zip(dates_idx, values)} for c in cols}
                else:
                    if flavor is None:
                        result = pd.DataFrame(data=values, index=dates_idx).to_json(date_format='iso')
                    elif flavor == 'pandas':
                        result = pd.DataFrame(data=values, index=dates_idx)
                    elif flavor == 'dict':
                        result = {0: {d: v for d, v in zip(dates_idx, values)}}
            else:
                result = values[0]
        except Exception as err:  # other error
            err_class = err.__class__.__name__
            detail = err.args[0]
            cl, exc, tb = sys.exc_info()
            line_number = traceback.extract_tb(tb)[-1][1]
            raise err
        else:
            exception = False  # no exceptions

        if exception:
            returncode = 1
            line_number -= 2
            errormsg = "%s at line %d: %s" % (err_class, line_number, detail)
            result = None
            raise SyntaxError(errormsg)
        else:
            returncode = 0
            errormsg = ''

        return returncode, errormsg, result

    def DATA(self, net_id, ref_key, ref_id, attr_id, date, counter):
        '''This sets self.data from self.calculate. It is only used if a function contains self.DATA, which is added by a preprocessor'''

        key = (net_id, ref_key, ref_id, attr_id)

        if key not in self.data:
            resource_scenario = self.conn.get_res_attr_data(
                ref_key=ref_key,
                ref_id=ref_id,
                scenario_id=[self.scenario_id],
                attr_id=attr_id
            )[0]
            try:
                eval_data = self.eval_data(
                    value=resource_scenario.value,
                    do_eval=False,
                    flavor='pandas',
                    counter=counter
                )
            except:
                raise
            self.data[key] = eval_data

        if self.data_type == 'timeseries':
            result = self.data[key].loc[date][0]
        else:
            result = self.data[key]

        return result
