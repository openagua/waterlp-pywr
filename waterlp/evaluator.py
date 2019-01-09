import hashlib
import json
import sys
import traceback
from copy import copy
from numpy import mean
from math import isnan, log
import pandas
from calendar import isleap

import pendulum

myfuncs = {}

EMPTY_VALUES = {
    'timeseries': {},
    'periodic timeseries': {},
    'scalar': 0,
    'array': [],
    'descriptor': ''
}


def get_scenarios_data(conn, scenario_ids, **kwargs):
    evaluator = Evaluator(
        conn,
        settings=kwargs.get('settings'),
        data_type=kwargs.get('data_type'),
        nblocks=kwargs.get('nblocks'),
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
    res_attr_data = evaluator.conn.get_res_attr_data(**kwargs)

    if res_attr_data and 'errorcode' not in res_attr_data:
        res_attr_data = res_attr_data[0]
        # evaluate the data
        # kwargs['data_type'] = res_attr_data.value.type

        eval_value = evaluator.eval_data(
            value=res_attr_data.value,
            do_eval=False,
            date_format=evaluator.date_format
        )
        if not eval_value:
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


def empty_data_timeseries(dates, nblocks=1, date_format='iso', flavor='json'):
    try:
        values = [0] * len(dates)
        timeseries = None
        if flavor == 'json':
            vals = {str(b): values for b in range(nblocks)}
            if date_format == 'iso':
                timeseries = pandas.DataFrame(vals, index=dates).to_json(date_format='iso')
            elif date_format == 'original':
                timeseries = pandas.DataFrame(vals, index=dates)
        elif flavor == 'pandas':
            timeseries = pandas.DataFrame(values, columns=range(nblocks), index=dates)
        else:
            vals = {b: values for b in range(nblocks)}
            timeseries = pandas.DataFrame(vals, index=dates).to_dict()
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


def eval_timeseries(timeseries, dates, fill_value=None, flatten=False, has_blocks=False, method=None, flavor=None,
                    date_format='iso'):
    try:
        df = pandas.read_json(timeseries)
        if df.empty:
            df = pandas.DataFrame(index=dates, columns=['0'])
        else:
            # df = df.reindex(pandas.DatetimeIndex(dates))
            if fill_value is not None:
                df.fillna(value=fill_value, inplace=True)
            elif method:
                df.fillna(method=method)

        if flavor == 'json':
            result = df.to_json(date_format=date_format)
        else:
            df.index = df.index.strftime(date_format)
            if flatten:
                df = df.sum(axis=1)
            result = df.to_dict()

        returncode = 0
        errormsg = 'No errors!'

        return returncode, errormsg, result
    except:
        raise


def eval_array(array, flavor=None):
    result = None
    try:
        temp = json.loads(array)
        if flavor is None:
            result = array
        elif flavor == 'list':
            result = temp
        elif flavor == 'pandas':
            result = pandas.DataFrame(temp)
        returncode = 0
        errormsg = 'No errors!'
    except:
        result = array
        returncode = 1
        errormsg = 'Something is wrong.'

    return returncode, errormsg, result


def parse_function(s, name, argnames):
    '''Parse a function into usable Python'''
    spaces = '\n    '

    # modules
    # modules = spaces.join('import {}'.format(m) for m in modules)

    # functions
    # functions = spaces.join(['{func} = self.{func}'.format(func=f) for f in functions])

    # getargs (these pass to self.GET)
    kwargs = spaces.join(['{arg} = kwargs.get("{arg}")'.format(arg=arg) for arg in argnames])

    # first cut
    s = s.rstrip()
    lines = s.split('\n')
    if 'return ' not in lines[-1]:
        lines[-1] = 'return ' + lines[-1]
    code = spaces.join(lines)

    # final function
    func = '''def {name}(self, **kwargs):{spaces}{kwargs}{spaces}{spaces}{code}''' \
        .format(spaces=spaces, kwargs=kwargs, code=code, name=name)

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
            dates = period.range("days")
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
            dates = period.range("months")
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
    if data_type == 'timeseries':
        default_eval_value = empty_data_timeseries(dates, nblocks=nblocks, flavor=flavor, date_format=date_format)
    elif data_type == 'periodic timeseries':
        dates = [pendulum.parse(d) for d in dates]
        periodic_dates = [d.replace(year=9999).to_datetime_string() for d in dates if (d - dates[0]).in_years() < 1]
        default_eval_value = empty_data_timeseries(
            periodic_dates,
            nblocks=nblocks,
            flavor=flavor,
            date_format=date_format
        )
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


class namespace:
    pass


class Evaluator:
    def __init__(self, conn=None, scenario_id=None, settings=None, date_format='iso', data_type='timeseries', nblocks=1):
        self.conn = conn
        self.dates_as_string, self.dates, self.periodic_timesteps = make_dates(settings, data_type=data_type)
        self.start_date = self.dates[0]
        self.end_date = self.dates[-1]
        self.scenario_id = scenario_id
        self.data_type = data_type
        self.date_format = date_format
        self.default_timeseries = make_default_value('timeseries', self.dates_as_string, flavor='dict',
                                                     date_format='original')
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
            'flavor'
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

    def eval_data(self, value, func=None, do_eval=False, flavor=None, depth=0, flatten=False, fill_value=None,
                  date_format='iso', has_blocks=False, data_type=None, parentkey=None):

        try:
            # create the data depending on data type

            returncode = None
            errormsg = None
            result = None

            # metadata = json.loads(resource_scenario.value.metadata)
            metadata = json.loads(value.get('metadata', '{}'))
            if func is None:
                func = metadata.get('function')
            usefn = metadata.get('use_function', 'N') == 'Y'
            data_type = data_type or value['type']

            if parentkey not in self.store:
                if data_type in ['timeseries', 'periodic timeseries']:
                    self.store[parentkey] = {}

            if usefn:
                func = func if type(func) == str else ''
                try:
                    returncode, errormsg, result = \
                        self.eval_function(
                            func,
                            flavor=flavor,
                            depth=depth,
                            has_blocks=has_blocks,
                            parentkey=parentkey,
                            data_type=data_type
                        )
                except InnerSyntaxError:
                    raise
                except Exception as e:
                    print(e)

            elif data_type == 'scalar':
                returncode, errormsg, result = eval_scalar(value.value)

            elif data_type == 'timeseries':

                returncode, errormsg, result = eval_timeseries(
                    value.value,
                    self.dates_as_string,
                    has_blocks=has_blocks,
                    flatten=(not has_blocks),
                    date_format=date_format,
                    fill_value=fill_value,
                    flavor=flavor
                )

            elif data_type == 'array':
                returncode, errormsg, result = eval_array(value.value)

            elif data_type == 'descriptor':
                returncode, errormsg, result = eval_descriptor(value.value)

            if do_eval:
                return returncode, errormsg, result
            else:
                if returncode:
                    raise Exception(errormsg)
                else:
                    # if data_type in ['timeseries', 'periodic timeseries']:
                    #     if not has_blocks or flatten:
                    #         if parentkey in self.store:
                    #             self.store[parentkey].update(result)
                    #     else:
                    #         for c, v in result.items():
                    #             if c not in self.store[parentkey]:
                    #                 self.store[parentkey][c] = {}
                    #             self.store[parentkey][c].update(v)
                    #
                    # else:
                    #     self.store[parentkey] = result
                    #
                    # return self.store[parentkey]
                    return result
        except:
            raise

    def eval_function(self, code_string, depth=0, parentkey=None, flavor=None, flatten=False, has_blocks=False, data_type=None):
        # This function is tricky. Basically, it should 1) return data in a format consistent with data_type
        # and 2) return everything that has previously been calculated, to aid in aggregation functions
        # The second goal is achieved by checking out results from the store

        # assume there will be an exception:
        err_class = None
        line_number = None
        exception = True
        result = None
        detail = None
        value = None

        hashkey = hashlib.sha224(str.encode(code_string + str(data_type))).hexdigest()

        # check if we already know about this function so we don't
        # have to do duplicate (possibly expensive) execs
        # if key not in self.myfuncs:
        if not hasattr(self.namespace, hashkey):
            try:
                # create the string defining the wrapper function
                # Note: functions can't start with a number so pre-pend "func_"
                func_name = "func_{}".format(hashkey)
                func = parse_function(code_string, name=func_name, argnames=self.argnames)
                # TODO : exec is unsafe
                exec(func, globals())
                # self.myfuncs[key] = func_name
                setattr(self.namespace, hashkey, eval(func_name))
            except Exception as e:
                print(e)
            except SyntaxError as err:  # syntax error
                err_class = err.__class__.__name__
                detail = err.args[0]
                line_number = err.lineno

        try:
            # CORE EVALUATION ROUTINE
            if hashkey not in self.hashstore:
                self.hashstore[hashkey] = [0 for d in self.dates]
            tsi = self.tsi
            tsf = self.tsf
            for i, date_as_string in enumerate(self.dates_as_string[tsi:tsf]):
                timestep = self.dates_as_string.index(date_as_string)
                date = self.dates[timestep]
                # value = getattr(self.namespace, hashkey)(
                #     self,
                #     hashkey=hashkey,
                #     date=date,
                #     timestep=timestep + 1,
                #     depth=depth + 1,
                #     flavor=flavor,
                #     parentkey=parentkey
                # )

                periodic_timestep = self.periodic_timesteps[timestep]
                water_year = date.year + (0 if date.month < self.start_date.month else 1)
                value = getattr(self.namespace, hashkey)(
                    self,
                    hashkey=hashkey,
                    date=date,
                    timestep=timestep + 1,
                    periodic_timestep=periodic_timestep,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    water_year=water_year,
                    depth=depth + 1,
                    parentkey=parentkey
                )

                if type(value) == dict and date_as_string in value:
                    val = value.get(date_as_string)
                else:
                    val = value

                if val and data_type in ['timeseries', 'periodic timeseries'] and isnan(val):
                    errormsg = "Attribute value is not a number."
                    raise Exception(errormsg)
                else:
                    self.hashstore[hashkey][timestep] = val
                if self.data_type != 'timeseries':
                    break

            values = self.hashstore[hashkey][tsi:]
            if data_type in ['timeseries', 'periodic timeseries']:
                dates_idx = self.dates_as_string[tsi:tsf]
                if type(values[0]) in (list, tuple):
                    cols = range(len(values[0]))
                    if flavor == 'json':
                        result = pandas.DataFrame.from_records(data=values, index=dates_idx,
                                                           columns=cols).to_json(date_format='iso')
                    elif flavor == 'pandas':
                        result = pandas.DataFrame.from_records(data=values, index=dates_idx, columns=cols)
                    else:
                        if has_blocks:
                            result = {c: {d: v[c] for d, v in zip(dates_idx, values)} for c in cols}
                        else:
                            result = {d: v[0] for d, v in zip(dates_idx, values)}
                else:
                    if flavor == 'json':
                        result = pandas.DataFrame(data=values, index=dates_idx).to_json(date_format='iso')
                    elif flavor == 'pandas':
                        result = pandas.DataFrame(data=values, index=dates_idx)
                    else:
                        result = self.store.get(parentkey, {})
                        if has_blocks and not flatten:
                            vals = {d: v for d, v in zip(dates_idx, values)}
                            for block in [0]:  # TODO: update this to include custom blocks
                                if block not in result:
                                    result[block] = {}
                                    result[block].update(vals)
                        else:
                            result = self.store.get(parentkey, {})
                            result.update({d: v for d, v in zip(dates_idx, values)})
            else:
                result = values[0]
        except Exception as err:  # other error
            err_class = err.__class__.__name__
            detail = err.args[0]
            cl, exc, tb = sys.exc_info()
            line_number = traceback.extract_tb(tb)[-1][1]
        else:
            exception = False  # no exceptions

        if exception:
            returncode = 1
            line_number -= 2
            errormsg = "%s: %s" % (err_class, detail)
            result = None
        else:
            returncode = 0
            errormsg = ''

        return returncode, errormsg, result

    def GET(self, key, **kwargs):
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
            flatten = kwargs.get('flatten', True)  # Flattens a variable with blocks. Defaults to True.
            start = kwargs.get('start')
            end = kwargs.get('end', date)
            agg = kwargs.get('agg', 'mean')

            parts = key.split('/')
            ref_key, ref_id, attr_id = parts
            ref_id = int(ref_id)
            attr_id = int(attr_id)

            result = None

            rs_value = self.rs_values.get((ref_key, ref_id, attr_id))

            # calculate offset
            if offset:
                offset_timestep = self.dates.index(date) + offset + 1
            else:
                offset_timestep = timestep

            if rs_value['type'] == 'timeseries':
                if key not in self.store:
                    self.store[key] = {}
                offset_date = self.dates[offset_timestep - 1]
                offset_date_as_string = offset_date.to_datetime_string()

                result = self.store[key].get(offset_date_as_string)

            flavor = kwargs.get('flavor')
            tattr = self.conn.tattrs[(ref_key, ref_id, attr_id)]
            has_blocks = tattr['properties'].get('has_blocks') or tattr['attr_name'] in self.block_params
            # need to evaluate the data anew only as needed
            # tracking parent key prevents stack overflow
            if key != parentkey and rs_value is not None \
                    and rs_value['value'] is not None and \
                    (not result or start):
                eval_data = self.eval_data(
                    value=rs_value,
                    do_eval=False,
                    flavor=flavor,
                    flatten=flatten,
                    depth=depth,
                    parentkey=key,
                    has_blocks=has_blocks,
                    data_type=tattr.data_type, # NOTE: the type attribute data type overrides the actual value type
                    date_format='%Y-%m-%d %H:%M:%S'
                )

                value = eval_data
            else:
                value = None

            result = value

            if self.data_type == 'timeseries':
                if rs_value['type'] == 'timeseries':

                    # store results from get function
                    # if key not in self.store:
                    #     self.store[key] = {}

                    if start:
                        if type(start) == str:
                            start = pendulum.parse(start)
                        if type(end) == str:
                            end = pendulum.parse(end)

                        if key != parentkey:
                            if flavor == 'pandas':
                                result = value.loc[start.to_datetime_string():end.to_datetime_string()].agg(agg)[0]
                            else:
                                idx_start = self.dates.index(start)
                                idx_end = self.dates.index(end)
                                if has_blocks:
                                    value = value[0]  # TODO: make this more sophisticated
                                values = list(value.values())[idx_start:idx_end]
                                if agg == 'sum':
                                    result = sum(values)
                                elif agg == 'mean':
                                    result = sum(values) / len(values)

                        else:
                            result = None

                    else:

                        # is the result already available?
                        result = self.store[key].get(offset_date_as_string)

                        if result is None:

                            if key == parentkey:
                                # this is for cases where we are getting from a previous time step in a top-level function
                                result = self.hashstore[hashkey][offset_timestep - 1]
                            else:
                                if flavor == 'pandas':
                                    if has_blocks:
                                        result = value.loc[offset_date_as_string]
                                    else:
                                        result = value.loc[offset_date_as_string][0]
                                else:
                                    if has_blocks:
                                        result = {c: value[c][offset_date_as_string] for c in value.keys()}
                                    else:
                                        result = value.get(offset_date_as_string) or value.get(0, {}).get(
                                            offset_date_as_string, 0)

                elif rs_value.type == 'array':

                    result = self.store.get(key)

                    if result is None:

                        if flavor == 'pandas':
                            result = pandas.DataFrame(value)
                        else:
                            result = value

                        # store results from get function
                        # self.store[key] = result

            return result

        except:
            raise

    def read_csv(self, path, **kwargs):

        kwargs.pop('date', None)
        kwargs.pop('hashkey', None)
        fullpath = '{}/{}'.format(self.network_files_path, path)

        data = self.external.get(fullpath)

        if data is None:
            for arg in self.argnames:
                exec("{arg} = kwargs.pop('{arg}', None)".format(arg=arg))

            flavor = kwargs.pop('flavor', 'dataframe')
            fill_method = kwargs.pop('fill_method', 'interpolate')
            interp_method = kwargs.pop('interp_method', None)

            df = pandas.read_csv(fullpath, **kwargs)

            interp_args = {}
            if fill_method == 'interpolate':
                if interp_method in ['time', 'akima', 'quadratic']:
                    interp_args['method'] = interp_method
                df.interpolate(inplace=True, **interp_args)

            if flavor == 'dict':
                data = df.to_dict()
            elif flavor == 'dataframe':
                data = df
            else:
                data = df

            self.external[fullpath] = data

        return data

    def call(self, *args, **kwargs):

        return 0
