import hashlib
import json
import sys
import traceback
from copy import copy
from calendar import isleap
import pandas
import numpy
import pendulum

# for use within user functions
from math import log, isnan

EMPTY_VALUES = {
    'timeseries': {},
    'periodic timeseries': {},
    'scalar': None,
    'array': None,
    'descriptor': None
}


def get_scenarios_data(conn, scenario_ids, **kwargs):
    evaluator = Evaluator(
        conn,
        settings=kwargs.get('settings'),
        data_type=kwargs.get('data_type'),
        nblocks=kwargs.get('nblocks'),
        date_format='%Y-%m-%d %H:%M:%S',
    )

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
    eval_value = None
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
        if eval_value is None:
            if evaluator.data_type:
                eval_value = evaluator.default_timeseries
            else:
                evaluator.data_type = res_attr_data.value.type
                evaluator.default_timeseries = make_default_value(
                    data_type=evaluator.data_type, dates=evaluator.dates_as_string, nblocks=1)
                eval_value = evaluator.default_timeseries

        metadata = json.loads(res_attr_data['value']['metadata'])
        metadata['use_function'] = metadata.get('use_function', 'N')
        # metadata['function'] = metadata.get('function', '')
        res_attr_data['value']['metadata'] = metadata

        scenario_data = {
            'data': res_attr_data,
            'eval_value': eval_value,
            'error': 0
        }

    else:
        data_type = kwargs.get('data_type')
        if data_type in ['timeseries', 'periodic timeseries']:
            eval_value = evaluator.default_timeseries
        elif data_type == 'array':
            eval_value = evaluator.default_array
        else:
            eval_value = None

        scenario_data = {'data': None, 'eval_value': eval_value, 'error': 1}

    scenario_data['id'] = evaluator.scenario_id

    return scenario_data


def empty_data_timeseries(dates, nblocks=1, flavor='json', date_format='iso'):
    try:
        timeseries = None
        values = [0] * len(dates)
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


def parse_function(s, name, argnames, modules=()):
    '''Parse a function into usable Python'''
    spaces = '\n    '

    # modules
    modules = spaces.join('import {}'.format(m) for m in modules)

    # getargs (these pass to self.GET)
    kwargs = spaces.join(['{arg} = kwargs.get("{arg}")'.format(arg=arg) for arg in argnames])

    # first cut
    s = s.rstrip()
    lines = s.split('\n')
    if 'return ' not in lines[-1]:
        lines[-1] = 'return ' + lines[-1]
    code = spaces.join(lines)

    # final function
    func = '''def {name}(self, **kwargs):{spaces}{modules}{spaces}{kwargs}{spaces}{code}''' \
        .format(spaces=spaces, modules=modules, kwargs=kwargs, code=code, name=name)

    return func


def make_dates(settings, date_format=True, data_type='timeseries'):
    # TODO: Make this more advanced
    timestep = settings.get('time_step') or settings.get('timestep')
    start = settings.get('start_time') or settings.get('start')
    end = settings.get('end_time') or settings.get('end')

    dates = []

    if start and end and timestep:
        start_date = pendulum.parse(start)
        end_date = pendulum.parse(end)
        timestep = timestep.lower()

        if data_type == 'periodic timeseries':
            start_date = pendulum.datetime(9998, 1, 1)
            end_date = pendulum.datetime(9998, 12, 31, 23, 59)

        period = pendulum.period(start_date, end_date)

        periodic_timesteps = []
        if timestep == 'day':
            dates = list(period.range("days"))
            pt = 0
            for i, date in enumerate(dates):
                if (date.month, date.day) == (start_date.month, start_date.day):
                    pt = 1
                else:
                    pt += 1
                periodic_timesteps.append(pt)
        elif timestep == 'week':
            year = start_date.year
            dates = []
            for i in range(52 * (end_date.year - start_date.year)):
                if i == 0:
                    date = start_date
                else:
                    date = dates[-1].add(days=7)
                if isleap(date.year) and date.month == 3 and date.day == 4:
                    date = date.add(days=1)
                if date.month == 12 and date.day == 31:
                    date = date.add(days=1)
                dates.append(date)
                periodic_timesteps.append(i % 52 + 1)
        elif timestep == 'month':
            dates = list(period.range("months"))
            periodic_timesteps = [i % 12 + 1 for i, date in enumerate(dates)]
        elif timestep == 'thricemonthly':
            dates = []
            for dt in period.range('months'):
                d1 = pendulum.datetime(dt.year, dt.month, 10)
                d2 = pendulum.datetime(dt.year, dt.month, 20)
                d3 = dt.last_of('month')
                dates.extend([d1, d2, d3])
            periodic_timesteps = [i % 36 + 1 for i, date in enumerate(dates)]

        dates_as_string = [date.to_datetime_string() for date in dates]

        return dates_as_string, dates, periodic_timesteps

    else:
        return None, None, None


def make_default_value(data_type='timeseries', dates=None, nblocks=1, flavor='json', date_format='iso'):
    try:
        if data_type == 'timeseries':
            default_eval_value = empty_data_timeseries(dates, nblocks=nblocks, flavor=flavor, date_format=date_format)
        elif data_type == 'periodic timeseries':
            dates = [pendulum.parse(d) for d in dates]
            periodic_dates = [d.replace(year=9999).to_datetime_string() for d in dates if (d - dates[0]).in_years() < 1]
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
    def __init__(self, conn=None, scenario_id=None, settings=None, data_type='timeseries', nblocks=1,
                 date_format='%Y-%m-%d %H:%M:%S'):
        self.conn = conn
        self.dates_as_string, self.dates, self.periodic_timesteps = make_dates(settings, data_type=data_type)
        self.date_format = date_format
        self.start_date = self.dates[0]
        self.end_date = self.dates[-1]
        self.scenario_id = scenario_id
        self.data_type = data_type
        self.default_timeseries = make_default_value(data_type=data_type, dates=self.dates_as_string, nblocks=nblocks)
        self.default_array = make_default_value('array')
        self.resource_scenarios = {}
        self.external = {}

        self.network_files_path = settings.get('network_files_path')

        self.namespace = namespace

        # arguments accepted by the function evaluator
        self.argnames = [
            'parentkey',
            'depth',
            'timestep',
            'periodic_timestep',
            'date',
            'start_date',
            'end_date',
            'water_year',
            'flavor',
        ]
        self.modules = ['pandas', 'isnan', 'log']

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
                    result = self.default_timeseries
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

        result = None
        date_format = date_format or self.date_format
        hashkey = hashlib.sha224(str.encode(code_string + str(data_type))).hexdigest()

        # check if we already know about this function so we don't
        # have to do duplicate (possibly expensive) execs
        if not hasattr(self.namespace, hashkey):
            try:
                # create the string defining the wrapper function
                # Note: functions can't start with a number so pre-pend "func_"
                func_name = "func_{}".format(hashkey)
                func = parse_function(code_string, name=func_name, argnames=self.argnames)
                # TODO : exec is unsafe
                exec(func, globals())
                setattr(self.namespace, hashkey, eval(func_name))
            except SyntaxError as err:  # syntax error
                print(err)
                raise
            except Exception as err:
                print(err)
                raise

        timestep = None

        try:
            # CORE EVALUATION ROUTINE

            stored_value = self.hashstore.get(hashkey)

            if stored_value is not None:
                if data_type != 'timeseries':
                    return self.hashstore[hashkey]

            # get dates to be evaluated
            if tsidx is not None:
                dates = self.dates[tsidx:tsidx + 1]
            else:
                tsi = getattr(self, 'tsi', None)
                tsf = getattr(self, 'tsf', None)
                if tsi is not None and tsf is not None:
                    dates = self.dates[tsi:tsf]  # used when running model
                else:
                    dates = self.dates  # used when evaluating a function in app

            for date in dates:
                date_as_string = date.to_datetime_string()
                timestep = self.dates_as_string.index(date_as_string) + 1
                periodic_timestep = self.periodic_timesteps[timestep - 1]
                water_year = date.year + (0 if date.month < self.start_date.month else 1)
                value = getattr(self.namespace, hashkey)(
                    self,
                    hashkey=hashkey,
                    date=date,
                    timestep=timestep,
                    periodic_timestep=periodic_timestep,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    water_year=water_year,
                    depth=depth + 1,
                    parentkey=parentkey,
                )

                if data_type == 'timeseries':
                    if hashkey not in self.hashstore:
                        self.hashstore[hashkey] = {}
                    if type(value) == dict and date_as_string in value:
                        self.hashstore[hashkey][date_as_string] = value.get(date_as_string)
                    elif type(value) in [pandas.DataFrame, pandas.Series]:
                        # TODO: add to documentation that returning a dataframe or series from a function
                        # will only be done once
                        if type(value.index) == pandas.DatetimeIndex:
                            value.index = value.index.strftime(date_format)
                        value = value.to_dict()
                        self.hashstore[hashkey] = value
                        break
                    else:
                        self.hashstore[hashkey][date_as_string] = value
                else:
                    self.hashstore[hashkey] = value
                    break

            values = self.hashstore[hashkey]

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
            if for_eval and timestep:
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

            hashkey = kwargs.get('hashkey')
            parentkey = kwargs.get('parentkey')
            date = kwargs.get('date')
            date_as_string = date.to_datetime_string()
            depth = kwargs.get('depth')
            offset = kwargs.get('offset')
            timestep = kwargs.get('timestep')
            flatten = kwargs.get('flatten', True)
            start = kwargs.get('start')
            end = kwargs.get('end')
            agg = kwargs.get('agg', 'mean')
            default = kwargs.get('default')

            parts = key.split('/')
            if len(parts) == 3:
                resource_type, resource_id, attr_id = parts
                network_id = None
            else:
                network_id, resource_type, resource_id, attr_id = parts
                network_id = int(resource_id)
                # network_id is not used yet - this is for future connections with other networks
            resource_id = int(resource_id)
            attr_id = int(attr_id)

            result = None
            value = None

            rs_value = self.rs_values.get((resource_type, resource_id, attr_id))
            if rs_value is None:
                return default

            # store results from get function
            if key not in self.store:
                self.store[key] = EMPTY_VALUES[rs_value['type']]

            # calculate offset
            offset_date_as_string = None
            if offset:
                offset_timestep = self.dates.index(date) + offset + 1
            else:
                offset_timestep = timestep

            if offset_timestep < 1 or offset_timestep > len(self.dates):
                pass
            elif not (start or end):  # TODO: change this when start/end are added to key
                stored_result = None
                if rs_value['type'] == 'timeseries':
                    offset_date = self.dates[offset_timestep - 1]
                    offset_date_as_string = offset_date.to_datetime_string()

                    stored_result = self.store[key].get(offset_date_as_string)

                elif rs_value['type'] in ['scalar', 'array', 'descriptor']:
                    stored_result = self.store[key]

                if stored_result is not None:
                    return stored_result

            default_flavor = None
            if rs_value.type == 'timeseries':
                default_flavor = 'native'
            elif rs_value.type == 'array':
                default_flavor = 'native'
            flavor = kwargs.get('flavor', default_flavor)

            tattr = self.conn.tattrs[(resource_type, resource_id, attr_id)]
            has_blocks = tattr['properties'].get('has_blocks')

            # need to evaluate the data anew only as needed
            # tracking parent key prevents stack overflow
            if key != parentkey:
                if rs_value is not None and rs_value['value'] is not None and (not result or start or end):
                    eval_data = self.eval_data(
                        value=rs_value,
                        flavor=flavor,
                        flatten=flatten,
                        depth=depth,
                        parentkey=key,
                        has_blocks=has_blocks,
                        tsidx=timestep - 1,  # convert from user timestep to python timestep
                        data_type=tattr.data_type,  # NOTE: the type attribute data type overrides the actual value type
                        date_format=self.date_format
                    )
                    self.store[key] = eval_data
                    value = eval_data

                else:

                    value = self.store[key]

            result = value

            if self.data_type == 'timeseries':
                if rs_value.type == 'timeseries':

                    if start or end:
                        start = start or date
                        end = end or date

                        if type(start) == str:
                            start = pendulum.parse(start)
                        if type(end) == str:
                            end = pendulum.parse(end)

                        if key != parentkey:
                            start_as_string = start.to_datetime_string()
                            end_as_string = end.to_datetime_string()
                            # start_as_string = start.to_iso8601_string().replace('Z', '')
                            # end_as_string = end.to_iso8601_string().replace('Z', '')
                            # Note annoying mismatch between Pandas and Pendulum iso8601 implementations
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
                            # is the result already available from a parent get result? or...
                            result = self.store.get(key, {}).get(offset_date_as_string)
                            if result is None:
                                # ...from the top-level function?
                                result = self.hashstore[hashkey][offset_date_as_string]

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

                elif rs_value.type == 'array':

                    result = self.store.get(key)

                    if result is None:

                        if flavor == 'pandas':
                            result = pandas.DataFrame(value)
                        else:
                            result = value

                elif rs_value.type in ['scalar', 'descriptor']:
                    result = value

            # TODO: double check if this is actually needed for timeseries...
            if rs_value.type in ['timeseries', 'periodic timeseries']:
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
                    pass # Error?
            else:
                self.store[key] = result

            return result if result is not None else default

        except:
            res_info = key
            raise Exception("Error getting data for key {}".format(res_info))

    def read_csv(self, path, **kwargs):

        date = kwargs.pop('date')
        date_as_string = date.to_datetime_string()
        hashkey = kwargs.pop('hashkey')
        fullpath = '{}/{}'.format(self.network_files_path, path)

        data = self.external.get(fullpath)

        if data is None:
            for arg in self.argnames:
                exec("{arg} = kwargs.pop('{arg}', None)".format(arg=arg))

            index_col = kwargs.pop('index_col', 0)
            parse_dates = kwargs.pop('parse_dates', True)
            flavor = kwargs.pop('flavor', 'dataframe')
            fill_method = kwargs.pop('fill_method', 'interpolate')
            interp_method = kwargs.pop('interp_method', None)

            df = pandas.read_csv(fullpath, index_col=index_col, parse_dates=parse_dates, **kwargs)

            interp_args = {}
            if fill_method == 'interpolate':
                if interp_method in ['time', 'akima', 'quadratic']:
                    interp_args['method'] = interp_method
                df.interpolate(inplace=True, **interp_args)

            if flavor == 'native':
                data = df.to_dict()
            elif flavor == 'dataframe':
                data = df
            else:
                data = df

            self.external[fullpath] = data

        return data

    def call(self, *args, **kwargs):

        return 0
