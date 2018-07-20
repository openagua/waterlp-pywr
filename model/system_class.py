from math import sqrt
import json
from collections import OrderedDict
import pandas as pd
from pyomo.environ import Var, Param
from datetime import datetime as dt

from evaluator import Evaluator


def convert_type_name(n):
    n = n.title()
    for char in [' ', '/', '-']:
        n = n.replace(char, '')
    return n


def convert_attr_name(n):
    n = n.title()
    for char in [' ', '/', '-']:
        n = n.replace(char, '')
    return n


def get_param_name(resource_type, attr_name):
    # IMPORTANT! rt-name combinations should be unique! This can be resolved in two ways:
    # 1. append the dimension to the param name, or
    # 2. use a unique internal name for all variables (e.g., reservoir_demand, urban_demand)
    # then use this unique internal name instead of the rt-name scheme.
    # Number two is var preferable

    return '{rt}{name}'.format(rt=resource_type.lower(), name=convert_attr_name(attr_name))


def perturb(val, variation):
    # NB: this is made explicit to avoid using exec
    operator = variation['operator']
    value = variation['value']
    if operator == 'multiply':
        if type(val) == dict:
            for c, vals in val.items():
                for i, v in vals.items():
                    if val[c][i] is not None:
                        val[c][i] *= value
        else:
            return val * value
    elif operator == 'add':
        if type(val) == dict:
            for c, vals in val.items():
                for i, v in vals.items():
                    if val[c][i] is not None:
                        val[c][i] = value

        else:
            return val + value
    else:
        return val


# class Recorder(object):
#     def __init__(self, flavor, host, port, database, username=None, password=None):
#
#         self.flavor = flavor
#         if flavor == 'mongodb':
#             client = MongoClient(host, port)
#             db = client[database]
#             self.results_collection = db['results']
#
#     def record(self, records):
#
#         if self.flavor == 'mongodb':
#             if type(records) != list:
#                 records = list(records)
#             try:
#                 self.results_collection.insert_many(records)
#             except:
#                 pass


def addsubblocks(values, param_name, subblocks):
    nsubblocks = len(subblocks)

    new_values = {}

    if param_name == 'nodeDemand':
        new_vals = {}
        try:
            for block in values:
                for d, v in values[block].items():
                    new_vals[d] = v / nsubblocks
                for i, subblock in enumerate(subblocks):
                    new_values[(block, subblock)] = new_vals
        except:
            raise

    elif param_name == 'nodePriority':
        for i, subblock in enumerate(subblocks):
            new_vals = {}
            for block in values:
                for d, v in values[block].items():
                    new_vals[d] = v + (1 - sqrt((nsubblocks - i) / nsubblocks))
                new_values[(block, subblock)] = new_vals

    return new_values


class WaterSystem(object):

    def __init__(self, conn, name, network, all_scenarios, template, attrs, args, settings=None, date_format='iso',
                 session=None, reporter=None, scenario=None):

        self.VOLUMETRIC_FLOW_RATE_CONST = 60 * 60 * 24 / 1e6
        self.ACRE_FEET_TO_VOLUME = 43559.9 / 1e6 * 1e3  # NOTE: model units are TAF, not AF

        self.conn = conn
        self.session = session
        self.name = name
        self.scenario = scenario
        self.template = template
        self.attrs = attrs
        self.reporter = reporter
        self.args = args
        self.date_format = date_format

        self.scenarios = {s.name: s for s in all_scenarios}
        self.scenarios_by_id = {s.id: s for s in all_scenarios}

        self.foresight = args.foresight  # pending further development

        # self.evaluator = Evaluator(self.conn, settings=settings, date_format=date_format)
        # self.dates = self.evaluator.dates
        # self.dates_as_string = self.evaluator.dates_as_string
        #
        # # timestep deltas
        # self.tsdeltas = {}
        #
        # # user the dates in evaluator because we've already incurred the expense of parsing the date.
        # self.tsdeltas = dict((self.dates_as_string[i], self.evaluator.dates[i + 1] - ts) for i, ts in
        #                      enumerate(self.evaluator.dates[:-1]))
        # self.tsdeltas[self.evaluator.dates_as_string[-1]] = self.tsdeltas[
        #     self.evaluator.dates_as_string[-2]]  # TODO: fix this

        # prepare data - we could move some of this to elsewhere

        template_id = template.id

        # extract info about nodes & links
        self.network = network
        self.nodes = {}
        self.links = {}
        self.ttypes = {'node': {}, 'link': {}, 'network': {}}
        self.res_attrs = {}
        self.link_nodes = {}

        self.params = {}  # to be defined later
        self.nparams = 0
        self.nvars = 0

        ttypeattrs = {'node': {}, 'link': {}, 'network': {}}
        rtypeattrs = {'node': {}, 'link': {}, 'network': {}}

        for tt in template.types:
            ttype_name = convert_type_name(tt.name)
            resource_type = tt.resource_type.lower()  # i.e., node, link, network
            self.ttypes[resource_type][ttype_name] = []

            ttypeattrs[resource_type][ttype_name] = [ta.attr_id for ta in
                                                     tt.typeattrs]  # use typeattrs to track variables/parameters

        # organize basic network information
        # features['networks'] = [network]
        for resource_type in ['node', 'link']:
            for resource in network['{}s'.format(resource_type)]:
                rtypes = list(filter(lambda x: x.template_id == template.id, resource.types))
                if not rtypes:
                    continue

                if resource_type == 'node':
                    idx = resource.id
                    self.nodes[idx] = resource
                elif resource_type == 'link':
                    idx = (resource.node_1_id, resource.node_2_id)
                    self.links[idx] = resource
                    self.link_nodes[resource.id] = idx

                # a dictionary of template_type to node_id
                rtype = rtypes[-1]
                type_name = convert_type_name(rtype.name)
                if type_name not in self.ttypes[resource_type]:
                    self.ttypes[resource_type][type_name] = []
                self.ttypes[resource_type][type_name].append(idx)

                rtypeattrs[resource_type][resource.id] = ttypeattrs[resource_type][type_name]

                # general resource attribute information
                for ra in resource.attributes:
                    attr_id = ra.attr_id
                    # if attr_id in list(attrs.keys()) + rtypeattrs[resource_type][resource.id]:
                    if attr_id in rtypeattrs[resource_type][resource.id]:
                        self.res_attrs[ra.id] = {
                            'name': attrs[attr_id]['name'],
                            'type': resource_type,
                            'data_type': 'timeseries',
                            'is_var': ra.attr_is_var
                        }

                        if ra.attr_is_var == 'N' and not args.suppress_input:
                            self.nparams += 1
                        else:
                            self.nvars += 1

        # initialize dictionary of parameters
        self.scalars = {feature_type: {} for feature_type in ['node', 'link', 'net']}

        self.ra_node = {ra.id: node.id for node in network.nodes for ra in node.attributes}  # res_attr to node lookup
        self.ra_link = {ra.id: link.id for link in network.links for ra in link.attributes}  # res_attr to link lookup

        # ra_net = dict() # res_attr to network lookup
        # for link in network.links:
        # for res_attr in link.attributes:
        # ra_link[res_attr.id] = link.id

        # may be useful in the future
        # self.class_lookup = {
        # 'node': 'nodes',
        # 'link': 'links',
        # 'network': 'network'
        # }

    def initialize_time_steps(self):

        # initialize time steps and foresight periods

        settings = {
            'start_time': self.scenario.start_time,
            'end_time': self.scenario.end_time,
            'time_step': self.scenario.time_step,
        }

        self.evaluator = Evaluator(self.conn, settings=settings, date_format=self.date_format)
        self.dates = self.evaluator.dates
        self.dates_as_string = self.evaluator.dates_as_string

        # timestep deltas
        self.tsdeltas = {}

        # user the dates in evaluator because we've already incurred the expense of parsing the date.
        self.tsdeltas = dict((self.dates_as_string[i], self.evaluator.dates[i + 1] - ts) for i, ts in
                             enumerate(self.evaluator.dates[:-1]))
        self.tsdeltas[self.evaluator.dates_as_string[-1]] = self.tsdeltas[
            self.evaluator.dates_as_string[-2]]  # TODO: fix this

        # NB: to be as efficient as possible within run loops, we should keep as much out of the loops as possible
        self.nruns = len(self.dates)
        if self.foresight == 'perfect':
            self.foresight_periods = len(self.dates)
            self.save_periods = self.foresight_periods
            self.nruns = 1
        elif self.foresight == 'zero':
            self.foresight_periods = 1
            self.save_periods = 1
        self.ts_idx = range(self.foresight_periods)

    def collect_source_data(self):
        """
        This does some pre-processing to organize data for more efficient lookup later.
        """

        tsi = 0
        tsf = self.foresight_periods

        self.timeseries = {}
        self.variables = {}
        self.block_params = ['Demand', 'Priority']
        self.blocks = {'node': {}, 'link': {}}
        self.results = {}
        self.res_scens = {}

        self.evaluator.block_params = self.block_params
        self.evaluator.rs_values = {}  # to store raw resource attribute values

        self.evaluator.tsi = tsi
        self.evaluator.tsf = tsf

        nsubblocks = 5
        self.default_subblocks = list(range(nsubblocks))

        # collect source data
        for source_id in self.scenario.source_ids:

            self.evaluator.scenario_id = source_id

            source = self.scenario.source_scenarios[source_id]

            for rs in source.resourcescenarios:

                # get identifiers
                if rs.resource_attr_id in self.ra_node:
                    resource_type = 'node'
                    resource_id = self.ra_node[rs.resource_attr_id]
                    idx = resource_id
                elif rs.resource_attr_id in self.ra_link:
                    resource_type = 'link'
                    resource_id = self.ra_link[rs.resource_attr_id]
                    idx = self.link_nodes[resource_id]
                else:
                    resource_type = 'network'
                    resource_id = self.network.id
                    idx = -1

                self.evaluator.rs_values[(resource_type, resource_id, rs.attr_id)] = rs.value

        for source_id in self.scenario.source_ids:

            self.evaluator.scenario_id = source_id

            source = self.scenario.source_scenarios[source_id]

            for rs in source.resourcescenarios:
                if rs.resource_attr_id not in self.res_attrs:
                    continue  # this is for a different resource type
                if self.res_attrs[rs.resource_attr_id]['is_var'] == 'Y':
                    continue  # this is a dependent (output) variable

                # create a dictionary to lookup resourcescenario by resource attribute ID
                self.res_scens[rs.resource_attr_id] = rs

                # load the metadata
                metadata = json.loads(rs.value.metadata)

                # get identifiers
                if rs.resource_attr_id in self.ra_node:
                    resource_type = 'node'
                    resource_id = self.ra_node[rs.resource_attr_id]
                    idx = resource_id
                elif rs.resource_attr_id in self.ra_link:
                    resource_type = 'link'
                    resource_id = self.ra_link[rs.resource_attr_id]
                    idx = self.link_nodes[resource_id]
                else:
                    resource_type = 'network'
                    idx = -1

                # identify as function or not
                is_function = metadata.get('use_function', 'N') == 'Y'

                # get attr name
                attr_name = self.res_attrs[rs.resource_attr_id]['name']

                # get data type
                data_type = rs.value.type

                # update data type
                self.res_attrs[rs.resource_attr_id]['data_type'] = data_type

                # default blocks
                # NB: self.block_params should be defined
                has_blocks = (attr_name in self.block_params) or metadata.get('has_blocks', 'N') == 'Y'
                blocks = [(0, 0)]

                param_name = get_param_name(resource_type, attr_name)

                # value = rs.value.value
                # TODO: get fill_value from dataset/ttype (this should be user-specified)
                self.evaluator.data_type = data_type
                try:
                    value = self.evaluator.eval_data(
                        value=rs.value,
                        do_eval=False,
                        flavor='dict',
                        fill_value=0,
                        has_blocks=has_blocks,
                        date_format=self.date_format
                    )
                except Exception as e:
                    raise

                if type(value) == str and not value:
                    continue

                # TODO: add generic unit conversion utility here
                dimension = rs.value.dimension

                if data_type == 'scalar':
                    try:
                        if param_name not in self.variables:
                            self.variables[param_name] = {}

                        value = float(value)  # TODO: add conversion?

                        self.variables[param_name][idx] = value
                    except:
                        # print('scalar problem')
                        pass

                elif data_type == 'descriptor':  # this could change later
                    if param_name not in self.variables:
                        self.variables[param_name] = {}
                    self.variables[param_name][idx] = value

                elif data_type == 'timeseries':
                    values = value
                    function = None

                    try:
                        if is_function:
                            function = metadata['function']
                            if not function:  # if there is no function, this will be treated as no dataset
                                continue
                        #     if has_blocks:
                        #         blocks = list(range(len(values)))
                        #
                        # else:
                        #
                        #     if has_blocks:
                        #         blocks = list(range(len(values)))

                        # routine to add blocks using quadratic values - this needs to be paired with a similar routine when updating boundary conditions
                        # if has_blocks and len(blocks) == 1:
                        if has_blocks:
                            values = addsubblocks(values, param_name, self.default_subblocks)
                            blocks = list(values.keys())

                        if param_name not in self.timeseries:
                            self.timeseries[param_name] = {}

                        self.timeseries[param_name][idx] = {
                            'data_type': data_type,
                            'values': values,
                            'is_function': is_function,
                            'function': function,
                            'has_blocks': has_blocks,
                        }
                    except:
                        raise

                # update resource blocks to match max of this type block and previous type blocks
                type_blocks = self.blocks[resource_type]
                if idx in type_blocks:
                    blocks = blocks if len(blocks) > len(type_blocks[idx]) else type_blocks[idx]
                self.blocks[resource_type][idx] = blocks

    def prepare_params(self):
        """
        Declare parameters, based on the template type.
        The result is a dictionary of all parameters for later use and extension.
        """

        for ttype in self.template.types:

            resource_type = ttype['resource_type']

            if resource_type == 'NETWORK':
                continue

            for type_attr in ttype.typeattrs:

                # data_type = type_attr['data_type']

                # create a unique parameter name
                param_name = get_param_name(resource_type, type_attr['attr_name'])

                if param_name in self.params:
                    continue

                self.params[param_name] = {
                    'attr_name': type_attr['attr_name'],
                    'attr_id': type_attr['attr_id'],
                    'type_attr': type_attr,
                    'is_var': type_attr['is_var'],
                    'resource_type': resource_type.lower(),
                    'unit': type_attr['unit'],
                    'dimension': type_attr['dimension']
                }

    def setup_subscenario(self, supersubscenario):
        """
        Add variation to all resource attributes as needed.
        There are two variations: option variations and scenario variations.
        If there is any conflict, scenario variations will replace option variations.
        """

        variation_sets = supersubscenario.get('variation_sets')

        self.metadata = {'number': supersubscenario.get('i'), 'variation_sets': {}}
        for i, variation_set in enumerate(variation_sets):
            vs = []
            for (resource_type, resource_id, attr_id), value in variation_set['variations'].items():
                vs.append({
                    'resource_type': resource_type,
                    'resource_id': resource_id,
                    'attr_id': attr_id,
                    'variation': value
                })
            scenario_type = 'option' if i == 0 else 'scenario'
            self.metadata['variation_sets'][scenario_type] = {
                'parent_id': variation_set['parent_id'],
                'variations': vs
            }

        for variation_set in variation_sets:
            for (resource_type, resource_id, attr_id), variation in variation_set['variations'].items():
                attr = self.conn.attrs[resource_type][attr_id]
                param_name = get_param_name(resource_type, attr['name'])
                if resource_type == 'node':
                    idx = resource_id
                elif resource_type == 'link':
                    idx = self.link_nodes[resource_id]
                # TODO: add other resource_types

                # at this point, timeseries have not been assigned to variables, so these are mutually exclusive
                # the order here shouldn't matter
                variable = self.variables.get(param_name, {}).get(idx)
                timeseries = self.timeseries.get(param_name, {}).get(idx)
                if variable:
                    self.variables[param_name][idx] = perturb(self.variables[param_name][idx], variation)

                elif timeseries:
                    if not timeseries.get('function'):  # functions will be handled by the evaluator
                        self.timeseries[param_name][idx]['values'] = perturb(self.timeseries[param_name][idx]['values'],
                                                                             variation)

                else:  # we need to add the variable to account for the variation
                    data_type = attr['dtype']
                    if data_type == 'scalar':
                        if param_name not in self.variables:
                            self.variables[param_name] = {}
                        self.variables[param_name][idx] = perturb(0, variation)
                    elif data_type == 'timeseries':

                        if param_name not in self.variables:
                            self.variables[param_name] = {}
                        self.timeseries[param_name][idx] = {
                            'values': perturb(self.evaluator.default_timeseries.copy(), variation),
                            'dimension': attr['dim']
                        }

    def init_pyomo_params(self):
        """Initialize Pyomo parameters with definitions."""

        for param_name, param in self.params.items():

            type_attr = param['type_attr']
            data_type = type_attr['data_type']
            resource_type = param['resource_type']
            attr_name = param['attr_name']
            unit = self.params[param_name]['unit']

            param_definition = None

            initial_values = self.variables.get(param_name, None)

            if param['is_var'] == 'N':

                mutable = True  # assume all variables are mutable
                default = 0  # TODO: define in template rather than here

                if data_type == 'scalar':
                    param_definition = 'm.{rt}s'

                    if unit == 'ac-ft':
                        initial_values = {key: value * self.ACRE_FEET_TO_VOLUME for (key, value) in
                                          initial_values.items()}

                elif data_type == 'timeseries':
                    if attr_name in self.block_params:
                        param_definition = 'm.{rt}Blocks, m.TS'
                    else:
                        param_definition = 'm.{rt}s, m.TS'

                elif data_type == 'array':
                    continue  # placeholder

                else:
                    # this includes descriptors, which have no place in the LP model yet
                    continue

                param_definition += ', default={}, mutable={}'.format(default, mutable)
                if initial_values is not None:
                    param_definition += ', initialize=initial_values'
                    # TODO: This is an opportunity for allocating memory in a Cythonized version?

                param_definition = param_definition.format(rt=resource_type.title())

            expression = 'm.{param_name} = Param({param_definition})'.format(
                param_name=param_name,
                param_definition=param_definition
            )

            self.params[param_name].update({
                'initial_values': initial_values,
                'expression': expression
            })
        return

    def update_initial_conditions(self):
        """Update initial conditions, such as reservoir and groundwater storage."""

        # we should provide a list of pairs to map variable to initial conditions (reservoir storage, groundwater storage, etc.)
        # Storage includes both reservoirs and groundwater
        for j in self.instance.Storage:
            getattr(self.instance, 'nodeInitialStorage')[j] = getattr(self.instance, 'nodeStorage')[j, 0].value

    def update_boundary_conditions(self, tsi, tsf, initialize=False):
        """Update boundary conditions. If initialize is True, this will create a variables object for use in creating the model (i.e., via init_pyomo_params). Otherwise, it will update the model instance."""

        dates_as_string = self.dates_as_string[tsi:tsf]

        for param_name, param in self.timeseries.items():
            for idx, p in param.items():

                try:

                    is_function = p.get('is_function')
                    has_blocks = p.get('has_blocks', False)
                    dimension = self.params[param_name]['dimension']
                    unit = self.params[param_name]['unit']

                    rt = self.params[param_name]['resource_type']
                    startup_date = self.variables.get('{}StartupDate'.format(rt), {}).get(idx, '')

                    if is_function:
                        self.evaluator.data_type = p['data_type']
                        self.evaluator.tsi = tsi
                        self.evaluator.tsf = tsf
                        try:
                            fn = p.get('function', '')
                            rc, errormsg, values = self.evaluator.eval_function(fn, counter=0, has_blocks=has_blocks)
                            if errormsg:
                                raise Exception(errormsg)
                        except:
                            raise

                        # update missing blocks, if any
                        # routine to add blocks using quadratic values - this needs to be paired with a similar routine when updating boundary conditions
                        if has_blocks:
                            values = addsubblocks(values, param_name, self.default_subblocks)

                    else:
                        # subblocks have already been added when reading in the time series
                        values = p['values']

                    if not values:
                        continue

                    if has_blocks:
                        cols = values.keys()
                    else:
                        cols = [0]
                    for j, c in enumerate(cols):

                        if has_blocks:
                            vals = values[c]
                        else:
                            vals = values.get(c, values)

                        # update values variable
                        for i, datetime in enumerate(dates_as_string):

                            if datetime not in vals:
                                continue

                            # set value of anything with a start date to zero
                            # note that this works to compare ISO-formatted strings, so no pendulum date needed
                            # TODO: make this more sophisticated
                            if datetime < startup_date:
                                val = 0

                            else:
                                val = vals[datetime]

                            # if is_function:
                            # TODO: use generic unit converter here (and move to evaluator?)
                            if dimension == 'Volumetric flow rate':
                                # if unit == 'ft^3 s^-1':
                                val *= self.tsdeltas[datetime].days * self.VOLUMETRIC_FLOW_RATE_CONST
                            elif dimension == 'Volume':
                                if unit == 'ac-ft':
                                    val *= self.ACRE_FEET_TO_VOLUME

                            # create key
                            key = list(idx) + [i] if type(idx) == tuple else [idx, i]
                            if has_blocks:
                                # add block & subblock to key
                                key.insert(-1, c[0])
                                key.insert(-1, c[1])
                            key = tuple(key)

                            if initialize:
                                if param_name not in self.variables:
                                    self.variables[param_name] = {}
                                self.variables[param_name][key] = val

                            else:  # just update the parameter directly
                                try:
                                    # TODO: replace this with explicit updates
                                    getattr(self.instance, param_name)[key] = val
                                except:
                                    pass  # likely the variable simply doesn't exist in the model
                except:
                    raise

    def update_internal_params(self):
        '''Update internal parameters based on calculated variables'''

        # define values based on user-defined priorities
        lowval = 100
        for idx in self.instance.nodePriority:
            getattr(self.instance, 'nodeValueDB')[idx] = \
                lowval - (getattr(self.instance, 'nodePriority')[idx].value or lowval)
        if hasattr(self.instance, 'linkPriority'):
            for idx in self.instance.linkPriority:
                getattr(self.instance, 'linkValueDB')[idx] = \
                    lowval - (getattr(self.instance, 'linkPriority')[idx].value or lowval)

    def collect_results(self, timesteps, tsidx, include_all=False, suppress_input=False):

        # loop through all the model parameters and variables
        if not suppress_input:
            for param in self.instance.component_objects(Param):
                self.store_results(param, timesteps, tsidx, is_var=False, include_all=include_all)

        for var in self.instance.component_objects(Var):
            self.store_results(var, timesteps, tsidx, is_var=True, include_all=include_all)

    def store_results(self, param, timesteps, tsidx, is_var, include_all=None):

        selfparam = self.params.get(param.name, {})
        if not selfparam and param.name not in ['debugLoss', 'debugGain']:
            return

        if tsidx == 0:
            self.results[param.name] = {}

        rt = selfparam.get('resource_type')
        has_blocks = selfparam.get('attr_name') in self.block_params
        dimension = selfparam.get('dimension')
        unit = selfparam.get('unit')

        # collect to results
        for idx, p in param.items():

            if param.name in ['debugLoss', 'debugGain']:
                if p.value:
                    res_idx = idx[0]
                    res_name = self.nodes[res_idx]['name']
                    raise Exception("DEBUG: {} for {} with value {}".format(param.name, res_name, p.value))
                else:
                    continue

            if is_var:

                # this assumes that all decision variables are time series
                # TODO: Verify this assumption
                # res_idx = len(idx)==2 and idx[0] or idx[:-1]
                res_idx = idx[:2] if rt == 'link' else idx[0]
                time_idx = idx[-1]
            else:
                res_idx = (type(idx) == int and idx) or (idx[:2] if rt == 'link' else idx[0])
                time_idx = type(idx) != int and idx[-1]

            if not (time_idx is not False and time_idx == 0 or include_all):  # index[-1] is time
                continue

            if tsidx == 0 and res_idx not in self.results[param.name]:  # idx[:-1] is node/link + block, if any
                self.results[param.name][res_idx] = {}

            timestamp = timesteps[time_idx]

            # the purpose of this addition is to aggregate blocks, if any, thus eliminating the need for Pandas
            # on the other hand, it should be checked which is faster: Pandas group_by or simple addition here

            val = 0 or round(p.value, 6)

            if dimension == 'Volume':
                if unit == 'ac-ft':
                    val /= self.ACRE_FEET_TO_VOLUME
            elif dimension == 'Volumetric flow rate':
                # elif unit == 'ft^3 s^-1':
                val /= (self.tsdeltas[timestamp].days * self.VOLUMETRIC_FLOW_RATE_CONST)

            if has_blocks:
                self.results[param.name][res_idx][timestamp] = \
                    val + self.results[param.name][res_idx].get(timestamp, 0)
            else:
                self.results[param.name][res_idx][timestamp] = val
                attr_id = self.params[param.name]['type_attr']['attr_id']
                if rt == 'node':
                    res_id = self.nodes[res_idx]
                else:
                    res_id = self.links[res_idx]['id']
                key = '{ref_key}/{ref_id}/{attr_id}'.format(ref_key=rt, ref_id=res_id, attr_id=attr_id)
                if key not in self.evaluator.results:
                    self.evaluator.results[key] = {}
                self.evaluator.results[key][timestamp] = val

        return

    def save_results(self):

        if self.scenario.reporter:
            self.scenario.reporter.report(action='save', saved=0)

        if self.args.destination == 'source':
            self.save_results_to_source()
        elif self.args.destination == 'aws_s3':
            self.save_results_to_s3()

    def save_results_to_source(self):

        result_scenario = self.scenarios.get(self.scenario.name)
        # if result_scenario and result_scenario.id not in self.scenario.source_ids:

        # self.conn.call('purge_scenario', {'scenario_id': result_scenario.id})
        # TODO: double check this routine. The result scenario should be re-used, so that any favorite can refer to the same scenario ID
        mod_date = dt.now().isoformat()
        if not result_scenario or result_scenario.id in self.scenario.source_ids:
            result_scenario = self.conn.call('add_scenario',
                                             {'network_id': self.network.id, 'scen': {
                                                 'id': None,
                                                 'name': self.scenario.name,
                                                 # 'cr_date': mod_date,
                                                 'description': '',
                                                 'network_id': self.network.id,
                                                 'layout': {
                                                     'class': 'results', 'sources': self.scenario.base_ids,
                                                     'tags': self.scenario.tags,
                                                     'modified_date': mod_date,
                                                     'modified_by': self.args.user_id
                                                 }
                                             }})
        else:
            result_scenario['layout'].update({
                'modified_date': mod_date,
                'modified_by': self.args.user_id
            })

            self.conn.call('update_scenario', {
                'scen': result_scenario,
            })

        # save variable data to database
        res_scens = []
        mb = 0
        res_names = {}

        try:
            count = 0
            pcount = 1
            nparams = len(self.results)
            for param_name, param_values in self.results.items():
                # if self.args.debug and pcount == 5:
                # break
                pcount += 1
                if param_name not in self.params:
                    continue  # it's probably an internal variable/parameter
                rt = self.params[param_name]['resource_type']
                ta = self.params[param_name]['type_attr']
                attr_id = ta['attr_id']
                attr = self.conn.attrs[rt][attr_id]

                # reorganize values as stored by Pyomo to resource attributes
                # pid = Pyomo resource attribute id
                dataset_values = {}
                for idx, values in param_values.items():
                    idx = type(idx) == tuple and list(idx) or [idx]  # needed to concatenate with the attribute name
                    if rt == 'node':
                        n = 1
                        pid = (idx[0], ta['attr_name'])
                        res_name = self.nodes[pid[0]]['name']
                    elif rt == 'link':
                        n = 2
                        pid = (idx[0], idx[1], ta['attr_name'])
                        res_name = self.links[pid[:n]]['name']
                    else:
                        # TODO: Include Network resource data here
                        continue

                    if pid not in self.conn.res_attr_lookup[rt]:
                        continue

                    # block = 0 if len(idx)==n else idx[n]
                    # if pid not in dataset_values:
                    # dataset_values[pid] = {}
                    # dataset_values[pid][str(block)] = values
                    dataset_values[pid] = values

                    if pid not in res_names: res_names[pid] = res_name

                # create datasets from values
                for pid, dataset_value in dataset_values.items():

                    count += 1

                    # define the dataset value
                    # first, aggregate blocks
                    # df = pd.DataFrame(data=dataset_value).sum(axis=1)
                    # df = pd.DataFrame(dataset_value)
                    value = json.dumps({'0': OrderedDict(sorted(dataset_value.items()))})

                    # create the resource scenario (dataset attached to a specific resource attribute)
                    rs = {
                        'resource_attr_id': self.conn.res_attr_lookup[rt][pid],
                        'value': {
                            'type': attr['dtype'],
                            'name': '{} - {} - {} [{}]'.format(self.network.name, res_names[pid], attr['name'],
                                                               self.scenario.name),
                            'unit': attr['unit'],
                            'dimension': attr['dim'],
                            'value': value
                        }
                    }
                    res_scens.append(rs)
                    mb += len(value.encode()) * 1.1 / 1e6  # large factor of safety

                    if mb > 10 or count % 100 == 0:
                        result_scenario['resourcescenarios'] = res_scens[:-1]
                        resp = self.conn.dump_results(result_scenario)
                        # if count % 20 == 0 or pcount == nparams:
                        if self.scenario.reporter:
                            self.scenario.reporter.report(
                                action='save',
                                saved=round(count / (self.nparams + self.nvars) * 100))

                        # purge just-uploaded scenarios
                        res_scens = res_scens[-1:]
                        mb = 0

            # upload the last remaining resource scenarios
            result_scenario['resourcescenarios'] = res_scens
            resp = self.conn.dump_results(result_scenario)
            if self.scenario.reporter:
                self.scenario.reporter.report(action='save', saved=round(count / (self.nparams + self.nvars) * 100))

        except:
            msg = 'ERROR: Results could not be saved.'
            # self.logd.info(msg)
            if self.scenario.reporter:
                self.scenario.reporter.report(action='error', message=msg)
            raise

    def save_results_to_s3(self):

        import boto3
        s3 = boto3.client('s3')

        if len(self.scenario.base_ids) == 1:
            o = s = self.scenario.base_ids[0]
        else:
            o, s = self.scenario.base_ids
        base_path = 'results/P{project}/N{network}/{scenario}/{run}/V{subscenario:05}'.format(
            project=self.network.project_id,
            network=self.network.id,
            run=self.args.start_time,
            scenario='O{}-S{}'.format(o, s),
            subscenario=self.metadata['number'])

        # save variable data to database
        res_scens = []
        res_names = {}

        try:

            # write metadata
            content = json.dumps(self.metadata, sort_keys=True, indent=4, separators=(',', ': ')).encode()
            s3.put_object(Body=content, Bucket='openagua.org', Key=base_path + '/metadata.json')

            count = 1
            pcount = 1
            nparams = len(self.results)
            path = base_path + '/data/{parameter}.csv'
            for param_name, param_values in self.results.items():
                pcount += 1
                if param_name not in self.params:
                    continue  # it's probably an internal variable/parameter
                rt = self.params[param_name]['resource_type']
                ta = self.params[param_name]['type_attr']
                attr_id = ta['attr_id']
                attr = self.conn.attrs[rt][attr_id]

                # reorganize values as stored by Pyomo to resource attributes
                # pid = Pyomo resource attribute id
                # create datasets from values
                df_all = pd.DataFrame()

                # dataset_values = {}
                for idx, values in param_values.items():
                    idx = type(idx) == tuple and list(idx) or [idx]  # needed to concatenate with the attribute name
                    if rt == 'node':
                        n = 1
                        pid = (idx[0], ta['attr_name'])
                        res_name = self.nodes[pid[0]]['name']
                    elif rt == 'link':
                        n = 2
                        pid = (idx[0], idx[1], ta['attr_name'])
                        res_name = self.links[pid[:n]]['name']
                    else:
                        # TODO: Include Network resource data here
                        continue

                    if pid not in self.conn.res_attr_lookup[rt]:
                        continue

                    # has_blocks = ta.properties.get('has_blocks') \
                    # or rt == 'node' and len(idx) == 2 \
                    # or rt == 'link' and len(idx) == 3

                    # if has_blocks:
                    # block = 0 if len(idx) == n else idx[n]
                    # df = pd.DataFrame.from_dict({(res_name, block): values})
                    # else:
                    df = pd.DataFrame.from_dict({res_name: values})
                    df_all = pd.concat([df_all, df], axis=1)

                # summed = df_all.groupby(axis=1, level=0).sum()
                content = df_all.to_csv().encode()

                s3.put_object(Body=content, Bucket='openagua.org', Key=path.format(parameter=param_name))

                if count % 10 == 0 or pcount == nparams:
                    if self.scenario.reporter:
                        self.scenario.reporter.report(action='save', progress=100,
                                                      saved=round(count / (self.nparams + self.nvars) * 100))
                count += 1

        except:
            msg = 'ERROR: Results could not be saved.'
            # self.logd.info(msg)
            if self.scenario.reporter:
                self.scenario.reporter.report(action='error', message=msg)
            raise
