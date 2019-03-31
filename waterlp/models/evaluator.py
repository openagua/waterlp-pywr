import hashlib
from os import environ
import json
import sys
import traceback
from copy import copy
from calendar import isleap
import pandas
import numpy
from pandas import Timestamp
import boto3
from io import BytesIO
from datetime import datetime

from ast import literal_eval

# for use within user functions
from math import log, isnan
import random

EMPTY_VALUES = {
    'timeseries': {},
    'periodic timeseries': {},
    'scalar': None,
    'array': None,
    'descriptor': None
}


def get_scenarios_data(conn, scenario_ids, **kwargs):
    evaluator = Evaluator(conn, **kwargs)

    scenarios_data = []
    for i, scenario_id in enumerate(scenario_ids):
        evaluator.scenario_id = scenario_id
        scenario_data = get_scenario_data(evaluator, **kwargs)

        # scenario_data['note'] = ''

        if scenario_data['data'] is None and i:
            scenario_data = copy(scenarios_data[i - 1])
            scenario_data.update({
                'id': scenario_id,
                'data': None,
                'error': 2,
            })

        elif scenario_data['data']:
            scenario_data['note'] = scenario_data['data']['value']['metadata'].get('note', '')

        scenarios_data.append(scenario_data)

    return scenarios_data


def get_scenario_data(evaluator, **kwargs):
    kwargs['scenario_id'] = [evaluator.scenario_id]
    for_eval = kwargs.get('for_eval', False)
    res_attr_data = evaluator.conn.get_res_attr_data(**kwargs)
    data_type = kwargs.get('data_type')
    eval_value = None
    error = 0
    if res_attr_data and 'errorcode' not in res_attr_data:
        res_attr_data = res_attr_data[0]

        parentkey = '{}/{}/{}'.format(
            kwargs.get('resource_type'),
            kwargs.get('resource_id'),
            kwargs.get('attr_id')
        )

        try:
            eval_value = evaluator.eval_data(
                parentkey=parentkey,
                value=res_attr_data.value,
                data_type=kwargs.get('data_type'),
                flatten=False,
                for_eval=for_eval,
            )
        except:
            eval_value = None
            error = 1

        if eval_value is None:
            eval_value = make_default_value(data_type=data_type, dates=evaluator.dates)
        elif type(eval_value) in [int, float] and 'timeseries' in data_type:
            eval_value = make_default_value(data_type=data_type, dates=evaluator.dates, default_value=eval_value)

        metadata = json.loads(res_attr_data['value']['metadata'])
        metadata['use_function'] = metadata.get('use_function', 'N')
        # metadata['function'] = metadata.get('function', '')
        res_attr_data['value']['metadata'] = metadata

        scenario_data = {
            'data': res_attr_data,
            'eval_value': eval_value,
            'error': error
        }

    else:
        if data_type in ['timeseries', 'periodic timeseries']:
            eval_value = make_default_value(data_type, evaluator.dates)
        elif data_type == 'array':
            eval_value = make_default_value(data_type)
        else:
            eval_value = None

        scenario_data = {'data': None, 'eval_value': eval_value, 'error': 1}

    scenario_data['id'] = evaluator.scenario_id

    return scenario_data


def empty_data_timeseries(dates, nblocks=1, flavor='json', date_format='iso', default_value=0):
    try:
        timeseries = None
        values = [default_value] * len(dates)
        if flavor == 'json':
            vals = {str(b): values for b in range(nblocks or 1)}
            if date_format == 'iso':
                timeseries = pandas.DataFrame(vals, index=dates).to_json(date_format='iso')
            elif date_format == 'original':
                timeseries = pandas.DataFrame(vals, index=dates)
        elif flavor == 'native':
            vals = {b: values for b in range(nblocks)}
            timeseries = pandas.DataFrame(vals, index=dates).to_dict()
        elif flavor == 'pandas':
            dates = pandas.to_datetime(dates)
            timeseries = pandas.DataFrame([[v] * nblocks for v in values], columns=range(nblocks), index=dates)
            timeseries.index.name = 'date'
        return timeseries
    except:
        raise


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
        raise Exception(errormsg)

    return x


def eval_descriptor(s):
    return s


def eval_timeseries(timeseries, dates, fill_value=None, fill_method=None, flatten=False, has_blocks=False, flavor=None,
                    date_format='%Y-%m-%d %H:%M:%S'):
    try:

        df = pandas.read_json(timeseries)
        if df.empty:
            df = pandas.DataFrame(index=dates, columns=['0'])
        else:
            # TODO: determine if the following reindexing is needed; it's unclear why it was added
            # this doesn't work with periodic timeseries, as Pandas doesn't like the year 9999
            # df = df.reindex(pandas.DatetimeIndex(dates))
            if fill_value is not None:
                df.fillna(value=fill_value, inplace=True)
            elif fill_method:
                df.fillna(method=fill_method)

        result = None
        if flatten:
            df = df.sum(axis=1)

        if flavor == 'pandas':
            result = df
        elif flavor == 'native':
            df.index = df.index.strftime(date_format=date_format)
            result = df.to_dict()
        elif flavor == 'json':
            result = df.to_json(date_format='iso')
        else:
            result = df.to_json(date_format='iso')

    except:

        returncode = -1
        errormsg = 'Error parsing timeseries data'
        raise Exception(errormsg)

    return result


def eval_array(array, flavor=None):
    result = None
    try:
        array_as_list = json.loads(array)
        if flavor is None:
            result = array
        elif flavor == 'native':
            result = array_as_list
        elif flavor == 'pandas':
            result = pandas.DataFrame(array_as_list)
        return result
    except:
        errormsg = 'Something is wrong.'
        raise Exception(errormsg)


def parse_function(user_code, name, argnames, modules=()):
    '''Parse a function into usable Python'''

    # first, parse code
    spaces = '\n    '
    s = user_code.rstrip()
    lines = s.split('\n')
    if 'return ' not in lines[-1]:
        lines[-1] = 'return ' + lines[-1]
    code = spaces.join(lines)

    try:
        eval1 = eval(user_code)
        eval2 = eval(user_code)
        if user_code and eval1 == eval2:
            return '''def {name}(self):{spaces}{code}'''.format(spaces=spaces, code=code, name=name)
    except:
        pass

    # modules
    # modules = spaces.join('import {}'.format(m) for m in modules if m in user_code)

    # getargs (these pass to self.GET)
    kwargs = spaces.join(['{arg} = kwargs.get("{arg}")'.format(arg=arg) for arg in argnames if arg in user_code])

    # final function
    func = '''def {name}(self, **kwargs):{spaces}{spaces}{kwargs}{spaces}{code}''' \
        .format(spaces=spaces, kwargs=kwargs, code=code, name=name)

    return func


class Timestep(object):
    index = -1
    periodic_timestep = 1

    def __init__(self, date, start_date, span):
        if date == start_date:
            type(self).index = 0
            type(self).periodic_timestep = 1
        else:
            type(self).index += 1
        self.index = type(self).index
        self.timestep = self.index + 1
        self.date = date
        self.year = date.year
        self.month = date.month
        self.day = date.day
        self.date_as_string = date.isoformat(' ')

        if start_date:
            if date.month < start_date.month:
                self.water_year = date.year
            else:
                self.water_year = date.year + 1

        if span:
            self.span = span
            self.set_periodic_timestep(date, start_date, span)

    def set_periodic_timestep(self, date, start_date, span):

        if span == 'day':
            if (date.month, date.day) == (start_date.month, start_date.day):
                type(self).periodic_timestep = 1
            else:
                type(self).periodic_timestep += 1
            self.periodic_timestep = type(self).periodic_timestep

        elif span == 'week':
            self.periodic_timestep = self.index % 52 + 1

        elif span == 'month':
            self.periodic_timestep = self.index % 12 + 1

        elif span == 'thricemonthly':
            self.periodic_timestep = self.index % 36 + 1


def make_timesteps(data_type='timeseries', debug_start=None, **kwargs):
    # TODO: Make this more advanced

    span = kwargs.get('span') or kwargs.get('timestep') or kwargs.get('time_step')
    start = kwargs.get('start') or kwargs.get('start_time')
    end = kwargs.get('end') or kwargs.get('end_time')

    timesteps = []

    if start and end and span:
        start_date = pandas.to_datetime(start)
        end_date = pandas.to_datetime(end)
        span = span.lower()

        if debug_start:
            start_date = max(start_date, pandas.to_datetime(debug_start))

        if data_type == 'periodic timeseries':
            start_date = pandas.datetime(9998, 1, 1)
            end_date = pandas.datetime(9998, 12, 31, 23, 59)

        if span == 'day':
            date_range = pandas.date_range(start=start_date, end=end_date, freq='D')
            timesteps = [Timestep(d, start_date, span) for d in date_range]
        elif span == 'week':
            dates = []
            for i in range(52 * (end_date.year - start_date.year)):
                if i == 0:
                    date = start_date
                else:
                    date = dates[-1] + pandas.DateOffset(days=7)
                if isleap(date.year) and date.month == 3 and date.day == 4:
                    date += pandas.DateOffset(days=1)
                if date.month == 12 and date.day == 31:
                    date += pandas.DateOffset(days=1)
                dates.append(date)
            timesteps = [Timestep(date, start_date, 'week') for date in dates]
        elif span == 'month':
            timesteps = [Timestep(d, start_date, 'month') for d in pandas.date_range(start=start, end=end, freq='M')]
        elif span == 'thricemonthly':
            dates = []
            for date in pandas.date_range(start=start_date, end=end_date, freq='M'):
                d1 = pandas.datetime(date.year, date.month, 10)
                d2 = pandas.datetime(date.year, date.month, 20)
                d3 = pandas.datetime(date.year, date.month, date.daysinmonth)
                dates.extend([d1, d2, d3])
            timesteps = [Timestep(d, start_date, span) for d in dates]

    return timesteps


def make_default_value(data_type='timeseries', dates=None, nblocks=1, default_value=0, flavor='json',
                       date_format='iso'):
    try:
        if data_type == 'timeseries':
            default_eval_value = empty_data_timeseries(dates, nblocks=nblocks, flavor=flavor, date_format=date_format,
                                                       default_value=default_value)
        elif data_type == 'periodic timeseries':
            periodic_dates = [d.replace(year=1678).to_datetime_string() for d in dates if (d - dates[0]).in_years() < 1]
            default_eval_value = empty_data_timeseries(periodic_dates, nblocks=nblocks)
        elif data_type == 'array':
            default_eval_value = '[[],[]]'
        else:
            default_eval_value = ''
        return default_eval_value
    except:
        raise


class InnerSyntaxError(SyntaxError):
    """Exception for syntax errors that will be defined only where the SyntaxError is made.

    Attributes:
        expression -- input expression in which the error occurred
        message    -- explanation of the error
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


class EvalException(Exception):
    def __init__(self, message, code):
        self.message = message
        self.code = code


class namespace:
    pass


class Evaluator:
    def __init__(self, conn=None, scenario_id=None,
                 time_settings=None, data_type='timeseries', debug_ts=None, debug_start=None, nblocks=1,
                 files_path=None, date_format='%Y-%m-%d %H:%M:%S', **kwargs):
        self.conn = conn

        self.dates = []
        self.dates_as_string = []
        self.timesteps = []
        self.start_date = None
        self.end_date = None

        if data_type in [None, 'timeseries', 'periodic timeseries']:
            timesteps = make_timesteps(data_type=data_type, debug_start=debug_start, **time_settings)
            if debug_ts:
                timesteps = timesteps[:debug_ts]
            self.timesteps = timesteps
            self.dates = [t.date for t in self.timesteps]
            self.dates_as_string = [t.date_as_string for t in self.timesteps]
            self.start_date = self.dates[0].date
            self.end_date = self.dates[-1].date

        self.date_format = date_format
        self.tsi = None
        self.tsf = None
        self.scenario_id = scenario_id
        self.data_type = data_type
        self.default_timeseries = None
        self.default_array = make_default_value('array')
        self.resource_scenarios = {}
        self.external = {}

        self.bucket_name = environ.get('AWS_S3_BUCKET')
        self.files_path = files_path

        self.namespace = namespace

        # arguments accepted by the function evaluator
        self.argnames = [
            'parentkey',
            'depth',
            'timestep',
            'date',
            'start_date',
            'end_date',
            'water_year',
            'flavor',
        ]
        self.modules = ['pandas', 'numpy', 'isnan', 'log', 'random', 'math']

        self.calculators = {}

        # This stores data that can be referenced later, in both time and space.
        # The main purpose is to minimize repeated calls to data sources, especially
        # when referencing other networks/resources/attributes within the project.
        # While this needs to be recreated on every new evaluation or run, within
        # each evaluation or run this can store as much as possible for reuse.
        self.store = {}
        self.hashstore = {}

    def eval_data(self, value, func=None, flavor=None, depth=0, flatten=False, fill_value=None,
                  tsidx=None, date_format=None, has_blocks=False, data_type=None, parentkey=None, for_eval=False):
        """
        Evaluate the data and return the appropriate value

        :param value:
        :param func:
        :param do_eval:
        :param flavor:
        :param depth:
        :param flatten:
        :param fill_value:
        :param date_format:
        :param has_blocks:
        :param data_type:
        :param parentkey:
        :return:
        """

        result = None
        date_format = date_format or self.date_format

        try:

            # metadata = json.loads(resource_scenario.value.metadata)
            metadata = json.loads(value.metadata)
            if func is None:
                func = metadata.get('function')
            usefn = metadata.get('use_function', 'N') == 'Y'
            data_type = data_type or value.type

            if usefn:
                func = func if type(func) == str else ''
                try:
                    result = self.eval_function(
                        func,
                        flavor=flavor,
                        depth=depth,
                        parentkey=parentkey,
                        data_type=data_type,
                        tsidx=tsidx,
                        has_blocks=has_blocks,
                        flatten=flatten,
                        date_format=date_format,
                        for_eval=for_eval
                    )
                except EvalException as err:
                    print(err.message)
                    raise
                except InnerSyntaxError as err:
                    print(err)
                    raise
                except Exception as err:
                    print(err)
                    raise
                if result is None and data_type == 'timeseries':
                    if self.default_timeseries:
                        result = self.default_timeseries
                    else:
                        self.default_timeseries = make_default_value(data_type=data_type, dates=self.dates)
                    if flavor == 'pandas':
                        result = pandas.read_json(result)
                    elif flavor == 'native':
                        result = json.loads(result)

            elif data_type == 'scalar':
                try:
                    eval_scalar(value.value)
                except:
                    raise

            elif data_type == 'descriptor':
                try:
                    eval_descriptor(value.value)
                except:
                    raise

            elif data_type in ['timeseries', 'periodic timeseries']:
                try:
                    result = eval_timeseries(
                        value.value,
                        self.dates_as_string,
                        has_blocks=has_blocks,
                        flatten=(flatten if flatten is not None else not has_blocks),
                        date_format=date_format,
                        fill_value=fill_value,
                        flavor=flavor,
                    )
                except:
                    raise

            elif data_type == 'array':
                try:
                    result = eval_array(
                        value.value,
                        flavor=flavor
                    )
                except:
                    raise

            return result

        except:
            raise

    def update_hashstore(self, hashkey, data_type, date_as_string, value):
        if data_type == 'timeseries':
            if hashkey not in self.hashstore:
                self.hashstore[hashkey] = {}
            if type(value) == dict and date_as_string in value:
                self.hashstore[hashkey][date_as_string] = value.get(date_as_string)
            elif type(value) in [pandas.DataFrame, pandas.Series]:
                # TODO: add to documentation that returning a dataframe or series from a function
                # will only be done once
                if type(value.index) == pandas.DatetimeIndex:
                    value.index = value.index.strftime(self.date_format)
                value = value.to_dict()
                self.hashstore[hashkey] = value
                return False
            else:
                self.hashstore[hashkey][date_as_string] = value
        else:
            self.hashstore[hashkey] = value
            return False

        return True

    def eval_function(self, code_string, depth=0, parentkey=None, flavor=None, data_type=None, flatten=False,
                      tsidx=None, has_blocks=False, date_format=None, for_eval=False):

        """
        This function is tricky. Basically, it should 1) return data in a format consistent with data_type
        and 2) return everything that has previously been calculated, to aid in aggregation functions
        The second goal is achieved by checking out results from the store.

        :param code_string:
        :param depth:
        :param parentkey:
        :param flavor:
        :param flatten:
        :param has_blocks:
        :param tsidx: Timestep index starting at 0
        :param data_type:
        :return:
        """

        hashkey = hashlib.sha224(str.encode(code_string + str(data_type))).hexdigest()

        # check if we already know about this function so we don't
        # have to do duplicate (possibly expensive) execs
        if not hasattr(self.namespace, hashkey):
            try:
                # create the string defining the wrapper function
                # Note: functions can't start with a number so pre-pend "func_"
                func_name = "func_{}".format(hashkey)
                func = parse_function(code_string, name=func_name, argnames=self.argnames, modules=self.modules)
                # TODO : exec is unsafe
                exec(func, globals())
                setattr(self.namespace, hashkey, eval(func_name))
            except SyntaxError as err:  # syntax error
                print(err)
                raise
            except Exception as err:
                print(err)
                raise

        i = -1

        try:
            # CORE EVALUATION ROUTINE

            stored_value = self.hashstore.get(hashkey)

            if stored_value is not None:
                if data_type != 'timeseries' or type(stored_value) in [float, int]:
                    return self.hashstore[hashkey]

            # MAIN ENTRY POINT TO FUNCTION
            f = getattr(self.namespace, hashkey)

            if data_type in ['scalar', 'array', 'descriptor']:
                value = f(self)
                self.hashstore[hashkey] = value

            else:

                # get dates to be evaluated
                # if tsidx is not None:
                #     timesteps = self.timesteps[tsidx:tsidx + 1]
                # elif for_eval:
                #     timesteps = self.timesteps  # used when evaluating a function in app
                # else:
                #     if self.tsi is not None and self.tsf is not None:
                #         timesteps = self.timesteps[self.tsi:self.tsf]  # used when running model
                #     else:
                #         raise Exception("Error evaluating function. Invalid dates.")
                timesteps = self.timesteps

                might_be_scalar = True
                for timestep in timesteps:
                    i += 1
                    try:
                        # if stored_value and date_as_string in stored_value:
                        #     value = stored_value[date_as_string]
                        # else:
                        if might_be_scalar and 'kwargs' not in f.__code__.co_varnames:
                            value = f(self)
                            if type(value) in [float, int]:
                                data_type = 'scalar'
                        else:
                            might_be_scalar = False
                            value = f(self, timestep=timestep, depth=depth + 1, parentkey=parentkey)
                        if self.update_hashstore(hashkey, data_type, timestep.date_as_string, value) is False:
                            break
                    except:
                        if for_eval:
                            break
                        else:
                            raise

            values = self.hashstore[hashkey]

            if type(values) in [int, float]:
                data_type = 'scalar'
            elif type(values) in [str]:
                data_type = 'descriptor'

            if data_type in ['timeseries', 'periodic timeseries']:
                if type(values) == list:
                    if flavor is 'json':
                        result = pandas.DataFrame(data=values, index=self.dates_as_string).to_json(date_format='iso')
                    elif flavor == 'native':
                        result = pandas.DataFrame(data=values, index=self.dates_as_string).to_dict()
                    elif flavor == 'pandas':
                        result = pandas.DataFrame(data=values, index=self.dates_as_string)
                    else:
                        result = pandas.DataFrame(data=values, index=self.dates_as_string).to_json(date_format='iso')

                elif type(values) == dict:
                    first_col = list(values.values())[0]
                    if type(first_col) in (list, tuple):
                        cols = range(len(values))
                        # TODO: add native flavor
                        if flavor == 'pandas':
                            result = pandas.DataFrame.from_records(data=values, index=self.dates_as_string,
                                                                   columns=cols)
                        elif flavor == 'native':
                            result = pandas.DataFrame.from_records(data=values, index=self.dates_as_string,
                                                                   columns=cols).to_dict()
                        else:
                            result = pandas.DataFrame.from_records(data=values, index=self.dates_as_string,
                                                                   columns=cols).to_json(date_format='iso')

                    else:
                        # if type(first_col) != dict:
                        #     values = {0: values}
                        if type(first_col) != dict and has_blocks:
                            values = {0: values}
                        elif type(first_col) == dict and not has_blocks:
                            values = first_col
                        if flavor is 'json':
                            result = pandas.DataFrame(data=values).to_json()
                        elif flavor == 'native':
                            result = values
                        elif flavor == 'pandas':
                            result = pandas.DataFrame(data=values)
                        else:
                            result = pandas.DataFrame(data=values).to_json()

                    if flatten:
                        if has_blocks:
                            flattened = {}
                            for col, vals in result.items():
                                for date, val in vals.items():
                                    flattened[date] = flattened.get(date, 0) + val
                            result = flattened

                else:
                    raise Exception("Incorrect data format for expression.")
            else:
                result = values

            return result

        except Exception as err:  # other error
            err_class = err.__class__.__name__
            detail = err.args[0]
            cl, exc, tb = sys.exc_info()
            line_number = traceback.extract_tb(tb)[-1][1]
            line_number -= 11
            errormsg = "%s at line %d: %s" % (err_class, line_number, detail)
            if for_eval and i > 0:
                errormsg += '\n\nThis error was encountered after the first time step, and might not occur during a model run.'
            # if for_eval:
            #     raise EvalException(errormsg, 3)
            # else:
            raise Exception(errormsg)

    def GET(self, key, **kwargs):
        """This is simply a pass-through to the newer, lowercase get"""
        return self.get(key, **kwargs)

    def get(self, key, **kwargs):
        '''
        This is used to get data from another variable, or another time step, possibly aggregated
        '''

        try:

            stored_result = self.store.get(key)

            if stored_result is not None:
                if type(stored_result) in [int, float, str, list]:
                    return stored_result

            parentkey = kwargs.get('parentkey')
            date = kwargs.get('date')
            depth = kwargs.get('depth')
            timestep = kwargs.get('timestep')
            flatten = kwargs.get('flatten', True)
            default = kwargs.get('default')

            result = None
            value = None
            start = None
            end = None
            offset_date_as_string = None

            # EXPENSIVE!!
            parts = key.split('/')
            if len(parts) == 3:
                resource_type, resource_id, attr_id = parts
                network_id = 0
            else:
                network_id, resource_type, resource_id, attr_id = parts
            resource_id = int(resource_id)
            attr_id = int(attr_id)

            rs_value = self.resource_scenarios.get(key)
            if rs_value is None:

                res_attr_data = self.conn.get_res_attr_data(
                    network_id=network_id,
                    resource_type=resource_type,
                    resource_id=resource_id,
                    scenario_id=[self.scenario_id],
                    attr_id=attr_id
                )
                if res_attr_data:
                    rs_value = res_attr_data[0].get('value')
                    self.resource_scenarios[key] = rs_value

            if rs_value is None:
                return default

            data_type = rs_value['type']

            # store results from get function
            if stored_result is None:
                self.store[key] = EMPTY_VALUES[rs_value['type']]
                stored_result = self.store[key]

            if 'timeseries' in data_type and timestep:

                offset = kwargs.get('offset')
                start = kwargs.get('start')
                end = kwargs.get('end')

                # calculate offset
                offset_date_as_string = None
                if offset:
                    offset_timestep = self.dates.index(date) + offset + 1
                    if offset_timestep < 1 or offset_timestep > len(self.dates):
                        raise Exception("Invalid offset")
                else:
                    offset_timestep = timestep.timestep

                if not (start or end):
                    if data_type in ['scalar', 'array', 'descriptor'] \
                            or type(stored_result) in [int, float, str, list]:
                        pass
                    elif data_type == 'timeseries':
                        offset_date_as_string = self.dates_as_string[offset_timestep - 1]
                        stored_result = stored_result.get(offset_date_as_string)
                    else:
                        pass

                    if stored_result is not None:
                        return stored_result

            default_flavor = None
            if data_type == 'timeseries':
                default_flavor = 'native'
            elif data_type == 'array':
                default_flavor = 'native'
            flavor = kwargs.get('flavor', default_flavor)

            tattr = self.conn.tattrs[(resource_type, resource_id, attr_id)]
            has_blocks = tattr['properties'].get('has_blocks')

            if key != parentkey:  # tracking parent key prevents stack overflows
                if rs_value is not None and rs_value['value'] is not None and (not result or start or end):
                    eval_data = self.eval_data(
                        value=rs_value,
                        flavor=flavor,
                        flatten=flatten,
                        depth=depth,
                        parentkey=key,
                        has_blocks=has_blocks,
                        tsidx=timestep and timestep.timestep - 1,  # convert from user timestep to python timestep
                        data_type=data_type,
                    )
                    self.store[key] = eval_data
                    value = eval_data

                else:

                    value = self.store[key]

            result = value

            if type(result) in [float, int]:
                data_type = 'scalar'

            if self.data_type == 'timeseries':
                if data_type == 'timeseries':

                    if start or end:
                        start = start or timestep.date
                        end = end or timestep.date

                        if type(start) == str:
                            start = pandas.to_datetime(start)
                        if type(end) == str:
                            end = pandas.to_datetime(end)

                        if key != parentkey:
                            start_as_string = start.isoformat(' ')
                            end_as_string = end.isoformat(' ')
                            agg = kwargs.get('agg', 'mean')
                            if default_flavor == 'pandas':
                                result = value.loc[start_as_string:end_as_string].agg(agg)[0]
                            elif default_flavor == 'native':
                                if flatten:
                                    values = value
                                else:
                                    values = list(value.values())[0]
                                vals = [values[k] for k in values.keys() if start_as_string <= k <= end_as_string]
                                if agg == 'mean':
                                    result = numpy.mean(vals)
                                elif agg == 'sum':
                                    result = numpy.sum(vals)
                        else:
                            result = None

                    elif offset_date_as_string:

                        if key == parentkey:
                            result = self.store.get(key, {}).get(offset_date_as_string)
                            if result is None:
                                raise Exception(
                                    "No result found for this variable for date {}".format(offset_date_as_string))

                        else:
                            if flavor == 'pandas':
                                if has_blocks:
                                    result = value.loc[offset_date_as_string]
                                else:
                                    result = value.loc[offset_date_as_string][0]

                            else:
                                if has_blocks and not flatten:
                                    # temp = value.get(0) or value.get('0') or {}
                                    # result = temp.get(offset_date_as_string)
                                    result = {c: value[c][offset_date_as_string] for c in value.keys()}
                                else:
                                    result = value.get(offset_date_as_string)

                elif data_type == 'array':

                    result = self.store.get(key)

                    if result is None:

                        if flavor == 'pandas':
                            result = pandas.DataFrame(value)
                        else:
                            result = value

                elif data_type in ['scalar', 'descriptor']:
                    result = value

            # TODO: double check if this is actually needed for timeseries...
            if data_type in ['timeseries', 'periodic timeseries']:
                if offset_date_as_string:
                    # TODO: account for the fact that key doesn't include start/end
                    # start/end should be added to key...
                    if has_blocks:
                        if 0 not in self.store[key]:
                            self.store[key] = {0: {}}
                        self.store[key][0][offset_date_as_string] = result
                    else:
                        self.store[key][offset_date_as_string] = result
                else:
                    pass  # Error?
            else:
                self.store[key] = result

            return result if result is not None else default

        except Exception as err:
            print(err)
            res_info = key
            raise Exception("Error getting data for key {}".format(res_info))

    def read_csv(self, path, **kwargs):

        externalkey = path + str(kwargs)
        data = self.external.get(externalkey)

        if data is None:

            kwargs['index_col'] = kwargs.pop('index_col', 0)
            kwargs['parse_dates'] = kwargs.pop('parse_dates', True)
            kwargs['infer_datetime_format'] = kwargs.pop('infer_datetime_format', True)
            flavor = kwargs.pop('flavor', 'dataframe')
            fill_method = kwargs.pop('fill_method', None)
            interp_method = kwargs.pop('interp_method', None)
            fit = kwargs.pop('fit', True)

            fullpath = '{}/{}'.format(self.files_path, path)
            path = 's3://{}/{}'.format(self.bucket_name, fullpath)

            try:
                # df = pandas.read_csv(path, **kwargs)
                client = boto3.client('s3')
                obj = client.get_object(Bucket=self.bucket_name, Key=fullpath)
                df = pandas.read_csv(BytesIO(obj['Body'].read()), **kwargs)
            except Exception as err:
                print("==============")
                print("Could not load csv file from path {}".format(path))
                print("kwargs: {}".format(kwargs))
                print("Original error:\n")
                print(err)
                print("==============")
                raise

            if fill_method:
                interp_args = {}
                if fill_method == 'interpolate':
                    if interp_method in ['time', 'akima', 'quadratic']:
                        interp_args['method'] = interp_method
                    df.interpolate(inplace=True, **interp_args)
            if fit and type(df.index) == pandas.DatetimeIndex:
                df = df.reindex(self.dates, fill_value=None)

            if flavor == 'native':
                data = df.to_dict()
            elif flavor == 'dataframe':
                data = df
            else:
                data = df

            self.external[externalkey] = data

        return data

    def call(self, *args, **kwargs):

        return 0
