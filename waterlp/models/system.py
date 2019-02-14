import os
from math import sqrt
import json
from collections import OrderedDict
import pandas as pd
import boto3
from datetime import datetime as dt

from waterlp.models.evaluator import Evaluator
from waterlp.utils.converter import convert


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
    # IMPORTANT! resource_type-name combinations should be unique! This can be resolved in two ways:
    # 1. append the dimension to the param name, or
    # 2. use a unique internal name for all variables (e.g., reservoir_demand, urban_demand)
    # then use this unique internal name instead of the resource_type-name scheme.
    # Number two is var preferable

    return '{resource_type}{name}'.format(resource_type=resource_type.lower(), name=convert_attr_name(attr_name))


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


def add_subblocks(values, param_name, subblocks):
    nsubblocks = len(subblocks)

    new_values = {}

    if param_name in ['nodeStorageDemand', 'nodeDemand']:
        new_vals = {}
        try:
            for block in values:
                for d, v in values[block].items():
                    new_vals[d] = v / nsubblocks
                for i, subblock in enumerate(subblocks):
                    new_values[(block, subblock)] = new_vals
        except:
            raise

    elif param_name in ['nodeValue', 'nodePriority', 'nodeViolationCost']:
        for block in values:
            for i, subblock in enumerate(subblocks):
                new_vals = {}
                for d, v in values[block].items():
                    new_vals[d] = v + (1 - sqrt((nsubblocks - i) / nsubblocks))
                new_values[(block, subblock)] = new_vals
    else:
        new_values = values

    return new_values


class WaterSystem(object):

    def __init__(self, conn, name, network, all_scenarios, template, args, date_format='iso',
                 session=None, reporter=None, scenario=None):

        self.storage = network.layout.get('storage')

        # Both of these are now converted to cubic meters (per time step)
        self.SECOND_TO_DAY = 60 * 60 * 24  # convert to million ft^3/day
        self.TAF_TO_VOLUME = 1e3 * 43560 / 1e6  # convert to million ft^3

        self.conn = conn
        self.session = session
        self.name = name
        self.scenario = scenario
        self.template = template
        self.reporter = reporter
        self.args = args
        self.date_format = date_format
        self.storage_scale = 1
        self.storage_unit = 'hm^3'

        self.scenarios = {s.name: s for s in all_scenarios}
        self.scenarios_by_id = {s.id: s for s in all_scenarios}

        self.foresight = args.foresight  # pending further development

        # extract info about nodes & links
        self.network = network
        self.nodes = {}
        self.links = {}
        self.ttypes = {}
        self.res_attrs = {}
        self.link_nodes = {}

        self.timeseries = {}
        self.variables = {}
        # self.block_params = ['Storage Demand', 'Demand', 'Priority']
        self.block_params = []
        self.blocks = {'node': {}, 'link': {}, 'network': {}}
        self.store = {}
        self.res_scens = {}

        self.params = {}  # to be defined later
        self.nparams = 0
        self.nvars = 0

        self.log_dir = 'log/{run_name}'.format(run_name=self.args.run_name)

        ttypeattrs = {}
        rtypeattrs = {'node': {}, 'link': {}, 'network': {}}

        for tt in template.types:
            ttype_name = convert_type_name(tt.name)
            resource_type = tt.resource_type.lower()  # i.e., node, link, network
            if resource_type not in self.ttypes:
                self.ttypes[resource_type] = {}
            self.ttypes[resource_type][ttype_name] = []

            # use typeattrs to track variables/parameters
            ttypeattrs[(resource_type, tt.name)] = tt.typeattrs

        # organize basic network information
        # features['networks'] = [network]

        def get_resource_attributes(resource):
            rtypes = [resource_type for resource_type in resource.types if resource_type.template_id == template.id]
            if not rtypes:
                return
            else:
                rtype = rtypes[0]

            if resource.get('nodes'):
                resource_type = 'network'
                idx = resource.id
            elif resource.get('node_1_id'):
                resource_type = 'link'
                idx = (resource.node_1_id, resource.node_2_id)
                self.links[idx] = resource
                self.link_nodes[resource.id] = idx
            else:
                resource_type = 'node'
                idx = resource.id
                self.nodes[idx] = resource

            # a dictionary of template_type to node_id
            type_name = convert_type_name(rtype.name)
            self.ttypes[resource_type][type_name].append(idx)

            tattrs = {ta.attr_id: ta for ta in ttypeattrs[(resource_type, rtype.name)]}

            res_idx = (resource_type, resource.id)
            rtypeattrs[res_idx] = list(tattrs.keys())

            # general resource attribute information
            for ra in resource.attributes:
                if ra.attr_id in rtypeattrs[res_idx]:
                    tattr = tattrs[ra.attr_id]
                    self.res_attrs[ra.id] = tattr

                    if ra.attr_is_var == 'N' and not args.suppress_input:
                        self.nparams += 1
                    else:
                        self.nvars += 1

        get_resource_attributes(network)
        for node in network.nodes:
            get_resource_attributes(node)
        for link in network.links:
            get_resource_attributes(link)

        # initialize dictionary of parameters
        self.scalars = {feature_type: {} for feature_type in ['node', 'link', 'net']}

        self.ra_node = {ra.id: node.id for node in network.nodes for ra in node.attributes}  # res_attr to node lookup
        self.ra_link = {ra.id: link.id for link in network.links for ra in link.attributes}  # res_attr to link lookup

    def create_exception(self, key, message):

        resource_type, resource_id, attr_id = key.split('/')
        resource_id = int(resource_id)
        attr_id = int(attr_id)
        attr_name = self.conn.tattrs.get((resource_type, resource_id, attr_id), {}).get('attr_name',
                                                                                        'unknown attribute')
        if resource_type == 'node':
            resource_name = self.nodes.get(resource_id, {}).get('name', 'unknown resource')
        elif resource_type == 'link':
            resource_name = self.links.get(self.link_nodes.get(resource_id), {}).get('name', 'unknown resource')
        else:
            resource_name = self.network['name']

        msg = 'Error calculating {attr} at {rtype} {res}:\n\n{exc}'.format(
            attr=attr_name,
            rtype=resource_type,
            res=resource_name,
            exc=message
        )

        return Exception(msg)

    def initialize_time_steps(self):
        # initialize time steps and foresight periods

        settings = {
            'start_time': self.scenario.start_time,
            'end_time': self.scenario.end_time,
            'time_step': self.scenario.time_step,
        }

        network_storage = self.conn.network.layout.get('storage')
        if network_storage.location == 'AmazonS3':
            network_folder = self.conn.network.layout.get('storage', {}).get('folder')
            bucket_name = os.environ.get('AWS_S3_BUCKET')

            settings['network_files_path'] = bucket_name and network_folder and 's3://{}/{}'.format(bucket_name,
                                                                                                    network_folder)

        self.evaluator = Evaluator(self.conn, settings=settings, date_format=self.date_format)
        self.dates = self.evaluator.dates
        self.dates_as_string = self.evaluator.dates_as_string

        # timestep deltas
        self.tsdeltas = {}

        # use the dates in evaluator because we've already incurred the expense of parsing the date.
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

        self.evaluator.block_params = self.block_params
        self.evaluator.rs_values = {}  # to store raw resource attribute values

        self.evaluator.tsi = tsi
        self.evaluator.tsf = tsf

        nsubblocks = 1
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
                elif rs.resource_attr_id in self.ra_link:
                    resource_type = 'link'
                    resource_id = self.ra_link[rs.resource_attr_id]
                else:
                    resource_type = 'network'
                    resource_id = self.network.id

                self.evaluator.rs_values[(resource_type, resource_id, rs.attr_id)] = rs.value

        # evaluate source data
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

                try:

                    res_attr = self.res_attrs.get(rs.resource_attr_id)
                    if not res_attr:
                        continue  # this is for a different resource type

                    intermediary = res_attr['properties'].get('intermediary')
                    is_var = res_attr['is_var'] == 'Y'

                    # non-intermediary outputs should not be pre-processed at all
                    # if is_var and not intermediary:
                    if is_var:
                        continue

                    # create a dictionary to lookup resourcescenario by resource attribute ID
                    self.res_scens[rs.resource_attr_id] = rs

                    # load the metadata
                    metadata = json.loads(rs.value.metadata)

                    # identify as function or not
                    is_function = metadata.get('use_function', 'N') == 'Y'

                    # get attr name
                    attr_name = res_attr['attr_name']
                    attr_id = res_attr['attr_id']

                    # get data type
                    data_type = rs.value.type

                    # update data type
                    self.res_attrs[rs.resource_attr_id]['data_type'] = data_type

                    # default blocks
                    # NB: self.block_params should be defined
                    # TODO: update has_blocks from template, not metadata
                    # has_blocks = attr_name in self.block_params or metadata.get('has_blocks', 'N') == 'Y'
                    has_blocks = False
                    blocks = [(0, 0)]

                    param_name = get_param_name(resource_type, attr_name)

                    parentkey = '{}/{}/{}'.format(resource_type, resource_id, attr_id)

                    # TODO: get fill_value from dataset/ttype (this should be user-specified)
                    self.evaluator.data_type = data_type
                    value = None
                    try:
                        # Intermediary output functions are not evaluated at this stage, as they may depend on calculated values
                        # if not (intermediary and is_var and is_function):
                        if not (is_var and is_function):
                            value = self.evaluator.eval_data(
                                value=rs.value,
                                do_eval=False,
                                fill_value=0,
                                has_blocks=has_blocks,
                                date_format=self.date_format,
                                flavor='dict',
                                parentkey=parentkey
                            )
                    except:
                        raise

                    if value is None or type(value) == str and not value:
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

                            # routine to add blocks using quadratic values - this needs to be paired with a similar routine when updating boundary conditions
                            if has_blocks:
                                values = add_subblocks(values, param_name, self.default_subblocks)

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

                    self.store[parentkey] = value

                    # update resource blocks to match max of this type block and previous type blocks
                    type_blocks = self.blocks[resource_type]
                    if idx in type_blocks:
                        blocks = blocks if len(blocks) > len(type_blocks[idx]) else type_blocks[idx]
                    self.blocks[resource_type][idx] = blocks

                except Exception as err:
                    if resource_type == 'node':
                        resource_name = self.nodes.get(resource_id, {}).get('name')
                    elif resource_type == 'link':
                        n1, n2 = self.link_nodes.get(resource_id)
                        resource_name = self.links.get((n1, n2), {}).get('name')
                    else:
                        resource_name = 'network'
                    msg = '{}\n\n{}'.format(
                        err,
                        'This error occurred when calculating {} for {}.'.format(rs['value']['name'], resource_name)
                    )

                    raise Exception(msg)

    def initialize(self, supersubscenario):
        """A wrapper for all initialization steps."""

        # add a store
        self.store = {}
        self.evaluator.store = self.store

        # prepare parameters
        self.prepare_params()

        # set up subscenario
        self.setup_subscenario(supersubscenario)

    def prepare_params(self):
        """
        Declare parameters, based on the template type.
        The result is a dictionary of all parameters for later use and extension.
        """

        for ttype in self.template.types:

            resource_type = ttype['resource_type']

            # if resource_type == 'NETWORK':
            #     continue

            for type_attr in ttype.typeattrs:

                # data_type = type_attr['data_type']

                # create a unique parameter name
                param_name = get_param_name(resource_type, type_attr['attr_name'])

                if param_name not in self.params:
                    param = type_attr
                    param.update(
                        resource_type=resource_type.lower(),
                        intermediary=type_attr['properties'].get('intermediary', False)
                    )
                    self.params[param_name] = param

                    if param_name == 'nodeInitialStorage':
                        self.storage_scale = param.properties.get('scale', 1)
                        self.storage_unit = param.unit

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
            for key, variation in variation_set['variations'].items():
                (resource_type, resource_id, attr_id) = key
                tattr = self.conn.tattrs[key]
                param_name = get_param_name(resource_type, tattr['attr_name'])
                idx = None
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
                    data_type = tattr['data_type']
                    if data_type == 'scalar':
                        if param_name not in self.variables:
                            self.variables[param_name] = {}
                        self.variables[param_name][idx] = perturb(0, variation)
                    elif data_type == 'timeseries':

                        if param_name not in self.variables:
                            self.variables[param_name] = {}
                        self.timeseries[param_name][idx] = {
                            'values': perturb(self.evaluator.default_timeseries.copy(), variation),
                            'dimension': tattr['dimension']
                        }

    def update_param(self, idx, param_name, dates_as_string, values=None, is_function=False, func=None,
                     has_blocks=False, step='main'):

        try:
            param = self.params[param_name]
            intermediary = param['intermediary']

            # if step == 'model' and scope in['pre-process' or scope == 'intermediary' and not intermediary:
            if step == 'main' and intermediary or step in ['pre-process', 'post-process'] and not intermediary:
                return

            dimension = param['dimension']
            data_type = param['data_type']
            unit = param['unit']
            attr_id = param['attr_id']
            resource_type = param['resource_type']
            startup_date = self.variables.get('{}StartupDate'.format(resource_type), {}).get(idx, '')

            if resource_type == 'network':
                resource = self.network
            elif resource_type == 'node':
                resource = self.nodes.get(idx)
            else:
                resource = self.links.get(idx)
            resource_id = resource.id
            parentkey = '{}/{}/{}'.format(resource_type, resource_id, attr_id)

            if is_function:
                self.evaluator.data_type = data_type
                try:
                    # full_key = (resource_type, resource_id, attr_id, dates_as_string)
                    rc, errormsg, values = self.evaluator.eval_function(
                        func,
                        has_blocks=has_blocks,
                        flatten=not has_blocks,
                        data_type=data_type,
                        parentkey=parentkey,
                        flavor='dict',
                    )
                    if errormsg:
                        raise Exception(errormsg)
                except Exception as err:
                    raise self.create_exception(parentkey, str(err))

                # update missing blocks, if any
                # routine to add blocks using quadratic values - this needs to be paired with a similar routine when updating boundary conditions
                if has_blocks:
                    values = add_subblocks(values, param_name, self.default_subblocks)

            if not values:
                return

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

                    # if scope == 'intermediary' and intermediary:
                    #     self.store_value(resource_type, resource_id, attr_id, datetime, val)
                    self.store_value(resource_type, resource_id, attr_id, datetime, val)

                    # elif scope == 'model' and not intermediary:

                    if val is not None:
                        scale = param.properties.get('scale', 1)
                        # only convert if updating the LP model
                        if dimension == 'Volumetric flow rate':
                            val = convert(val * scale, dimension, unit, 'hm^3 day^-1')
                        elif dimension == 'Volume':
                            val = convert(val * scale, dimension, unit, 'hm^3')

                    try:
                        if param_name == 'nodeRunoff':
                            self.model.non_storage[resource_id].min_flow = val
                            self.model.non_storage[resource_id].max_flow = val
                        elif param_name == 'nodeDemand':
                            if hasattr(self.model.non_storage[resource_id], 'mrf'):
                                self.model.non_storage[resource_id].mrf = val  # this is a flow requirement
                            else:
                                self.model.non_storage[resource_id].max_flow = val
                        elif param_name == 'nodeValue':
                            if resource_id in self.model.non_storage:
                                self.model.non_storage[resource_id].cost = -val
                            elif idx in self.model.storage:
                                self.model.storage[resource_id].cost = -val
                        elif param_name == 'nodeViolationCost':  # this is a flow requirement
                            # self.model.non_storage[resource_id].cost = 0.0
                            self.model.non_storage[resource_id].mrf_cost = -val
                        elif param_name == 'nodeTurbineCapacity':
                            self.model.non_storage[resource_id].max_flow = val
                        elif param_name == 'nodeStorageDemand':
                            self.model.storage[resource_id].max_volume = val
                        # elif param_name == 'nodeStorageCapacity':
                        #     self.model.storage[resource_id].max_volume = val
                        elif param_name == 'nodeInactivePool':
                            self.model.storage[resource_id].min_volume = val
                        elif param_name == 'linkFlowCapacity':
                            self.model.links[resource_id].max_flow = val

                    except:
                        pass  # unclear what the exception is
        except:
            raise

    def update_initial_conditions(self, variables=None, initialize=False):
        """Update initial conditions, such as reservoir and groundwater storage."""

        node_ids = list(self.model.storage.keys())

        if initialize:
            for node_id in node_ids:
                initial_volume = variables.get('nodeInitialStorage', {}).get(node_id, 0)
                initial_volume = convert(initial_volume * self.storage_scale, 'Volume', self.storage_unit, 'hm^3')
                self.model.storage[node_id].initial_volume = initial_volume

        else:
            for node_id, node in self.model.storage.items():
                node.initial_volume = node.volume[0]

        return

    def update_boundary_conditions(self, tsi, tsf, step='main'):
        """
        Update boundary conditions.
        """
        dates_as_string = self.dates_as_string[tsi:tsf]
        self.evaluator.tsi = tsi
        self.evaluator.tsf = tsf

        for param_name, params in self.timeseries.items():
            for idx, p in params.items():
                has_blocks = p.get('has_blocks', False)
                self.update_param(
                    idx,
                    param_name,
                    dates_as_string,
                    values=p.get('values'),
                    is_function=p.get('is_function'),
                    func=p.get('function'),
                    has_blocks=has_blocks,
                    step=step,
                )

    def collect_results(self, timesteps, tsidx, include_all=False, suppress_input=False):

        # loop through all the model parameters and variables
        for res_id, node in self.model.non_storage.items():
            self.store_results(
                res_id=res_id,
                param_name='nodeOutflow',
                timestamp=timesteps[0],
                value=node.flow[0],
            )

            self.store_results(
                res_id=res_id,
                param_name='nodeInflow',
                timestamp=timesteps[0],
                value=node.flow[0],
            )

        for res_id, node in self.model.storage.items():
            self.store_results(
                res_id=res_id,
                param_name='nodeStorage',
                timestamp=timesteps[0],
                value=node.volume[0],
            )
            self.store_results(
                res_id=res_id,
                param_name='nodeOutflow',
                timestamp=timesteps[0],
                value=sum([input.flow[0] for input in node.inputs]),  # "input" means "input to the system"
            )
            self.store_results(
                res_id=res_id,
                param_name='nodeInflow',
                timestamp=timesteps[0],
                value=sum([output.flow[0] for output in node.outputs]),
            )

    def store_results(self, res_id=None, param_name=None, timestamp=None, value=None):

        param = self.params.get(param_name, {})

        resource_type = param.get('resource_type')
        has_blocks = param.get('attr_name') in self.block_params
        dimension = param.get('dimension')
        unit = param.get('unit')
        scale = param.properties.get('scale', 1)
        attr_id = param.get('attr_id')

        # collect to results

        # the purpose of this addition is to aggregate blocks, if any, thus eliminating the need for Pandas
        # on the other hand, it should be checked which is faster: Pandas group_by or simple addition here

        if dimension == 'Volume':
            value = convert(value, dimension, 'hm^3', unit) / scale
        elif dimension == 'Volumetric flow rate':
            value = convert(value, dimension, 'hm^3 day^-1', unit) / scale

        # store in evaluator store
        self.store_value(resource_type, res_id, attr_id, timestamp, value, has_blocks=has_blocks)

    def store_value(self, ref_key, ref_id, attr_id, timestamp, val, has_blocks=False):

        # add new resource scenario if it doesn't exist
        key = (ref_key, ref_id, attr_id)
        try:
            if key not in self.evaluator.rs_values:
                tattr = self.conn.tattrs.get(key)
                if not tattr:
                    # This is because the model assigns all resource attribute possibilities to all resources of like type
                    # In practice this shouldn't make a difference, but may result in a model larger than desired
                    # TODO: correct this
                    return
                self.evaluator.rs_values[key] = {
                    'type': tattr['data_type'],
                    'unit': tattr['unit'],
                    'dimension': tattr['dimension'],
                    'value': None
                }
        except:
            raise

        # store value
        key_string = '{ref_key}/{ref_id}/{attr_id}'.format(ref_key=ref_key, ref_id=ref_id, attr_id=attr_id)
        if key_string not in self.store:
            if has_blocks:
                self.store[key_string] = {0: {}}
            else:
                self.store[key_string] = {}
        elif has_blocks and 0 not in self.store[key_string]:
            self.store[key_string][0] = {}
        if has_blocks:
            val += self.store[key_string][0].get(timestamp, 0)
            self.store[key_string][0][timestamp] = val
        else:
            self.store[key_string][timestamp] = val

    def save_logs(self):

        for filename in ['pywr_glpk_debug.lp', 'pywr_glpk_debug.mps']:
            if os.path.exists(filename):
                with open(filename, 'r') as file:
                    content = file.read()
                    self.save_to_file(filename, content)
            else:
                return None

        return True

    def save_to_file(self, filename, content):
        s3 = boto3.client('s3')
        key = '{network_folder}/{log_dir}/{filename}'.format(
            network_folder=self.storage.folder,
            log_dir=self.log_dir,
            filename=filename
        )
        s3.put_object(Body=content, Bucket=os.environ.get('AWS_S3_BUCKET'), Key=key)

    def save_results(self, error=False):

        if self.scenario.reporter:
            self.scenario.reporter.report(action='save', saved=0)

        if self.args.destination == 'source':
            self.save_results_to_source()
        elif self.args.destination == 'local':
            self.save_results_to_local()
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
                                                     'run': self.args.run_name,
                                                     'modified_date': mod_date,
                                                     'modified_by': self.args.user_id
                                                 }
                                             }})
        else:
            result_scenario['layout'].update({
                'modified_date': mod_date,
                'modified_by': self.args.user_id,
            })

            self.conn.call('update_scenario', {
                'scen': result_scenario,
            })

        # save variable data to database
        res_scens = []
        mb = 0
        res_names = {}

        try:
            n = 0
            N = len(self.store)
            for key, value in self.store.items():
                n += 1
                res_type, res_id, attr_id = key.split('/')
                res_id = int(res_id)
                attr_id = int(attr_id)

                tattr = self.conn.tattrs.get((res_type, res_id, attr_id))
                if not tattr:
                    # Same as previous issue.
                    # This is because the model assigns all resource attribute possibilities to all resources of like type
                    # In practice this shouldn't make a difference, but may result in a model larger than desired
                    # TODO: correct this
                    continue
                param_name = get_param_name(res_type, tattr['attr_name'])

                if param_name not in self.params:
                    continue  # it's probably an internal variable/parameter

                # define the dataset value
                try:
                    value = json.dumps({'0': OrderedDict(sorted(value.items()))})
                except:
                    continue

                # create the resource scenario (dataset attached to a specific resource attribute)
                res_idx = (res_type, res_id, attr_id)
                res_attr_id = self.conn.res_attr_lookup.get(res_idx)
                if not res_attr_id:
                    continue
                resource_name = self.conn.raid_to_res_name[res_attr_id]
                if res_type == 'network':
                    res_scen_name = '{} - {} [{}]'.format(self.network.name, tattr['attr_name'], self.scenario.name)
                else:
                    res_scen_name = '{} - {} - {} [{}]'.format(self.network.name,
                                                               resource_name,
                                                               tattr['attr_name'],
                                                               self.scenario.name)

                if tattr['dimension'] == 'Temperature':
                    continue  # TODO: fix this!!!

                rs = {
                    'resource_attr_id': res_attr_id,
                    'value': {
                        'type': tattr['data_type'],
                        'name': res_scen_name,
                        'unit': tattr['unit'],
                        'dimension': tattr['dimension'],
                        'value': value
                    }
                }
                res_scens.append(rs)
                mb += len(value.encode()) * 1.1 / 1e6  # large factor of safety

                if mb > 10 or n % 100 == 0:
                    result_scenario['resourcescenarios'] = res_scens[:-1]
                    resp = self.conn.dump_results(result_scenario)
                    if 'id' not in resp:
                        raise Exception('Error saving data')
                    if self.scenario.reporter:
                        self.scenario.reporter.report(
                            action='save',
                            saved=round(n / N * 100)
                        )

                    # purge just-uploaded scenarios
                    res_scens = res_scens[-1:]
                    mb = 0

            # upload the last remaining resource scenarios
            result_scenario['resourcescenarios'] = res_scens
            resp = self.conn.dump_results(result_scenario)

            if self.scenario.reporter:
                if N:
                    self.scenario.reporter.report(action='save', saved=round(n / N * 100))
                else:
                    self.scenario.reporter.report(action='error', message="ERROR: No results have been reported. The model might not have run.")

        except:
            msg = 'ERROR: Results could not be saved.'
            # self.logd.info(msg)
            if self.scenario.reporter:
                self.scenario.reporter.report(action='error', message=msg)
            raise

    # def save_results_to_s3(self):
    #
    #     import boto3
    #     s3 = boto3.client('s3')
    #
    #     if len(self.scenario.base_ids) == 1:
    #         o = s = self.scenario.base_ids[0]
    #     else:
    #         o, s = self.scenario.base_ids
    #     base_path = 'results/P{project}/N{network}/{scenario}/{run}/V{subscenario:05}'.format(
    #         project=self.network.project_id,
    #         network=self.network.id,
    #         run=self.args.start_time,
    #         scenario='O{}-S{}'.format(o, s),
    #         subscenario=self.metadata['number'])
    #
    #     # save variable data to database
    #     res_scens = []
    #     res_names = {}
    #
    #     try:
    #
    #         # write metadata
    #         content = json.dumps(self.metadata, sort_keys=True, indent=4, separators=(',', ': ')).encode()
    #         s3.put_object(Body=content, Bucket='openagua.org', Key=base_path + '/metadata.json')
    #
    #         count = 1
    #         pcount = 1
    #         nparams = len(self.store)
    #         path = base_path + '/data/{parameter}.csv'
    #         for param_name, param_values in self.store.items():
    #             pcount += 1
    #             df = self.param_to_df(param_name, param_values)
    #             content = df.to_csv().encode()
    #
    #             if content:
    #                 s3.put_object(Body=content, Bucket='openagua.org', Key=path.format(parameter=param_name))
    #
    #             if count % 10 == 0 or pcount == nparams:
    #                 if self.scenario.reporter:
    #                     self.scenario.reporter.report(action='save', saved=round(count / (self.nparams + self.nvars) * 100))
    #             count += 1
    #
    #     except:
    #         msg = 'ERROR: Results could not be saved.'
    #         # self.logd.info(msg)
    #         if self.scenario.reporter:
    #             self.scenario.reporter.report(action='error', message=msg)
    #         raise
    #
    def save_results_to_local(self):

        if len(self.scenario.base_ids) == 1:
            o = s = self.scenario.base_ids[0]
        else:
            o, s = self.scenario.base_ids
        base_path = './results/P{project}/N{network}/{scenario}/{run}/V{subscenario:05}'.format(
            project=self.network.project_id,
            network=self.network.id,
            run=self.args.start_time,
            scenario='O{}-S{}'.format(o, s),
            subscenario=self.metadata['number'])

        if not os.path.exists(base_path):
            os.makedirs(base_path)

        res_names = {
            'node': {n.id: n.name for n in self.network.nodes},
            'link': {l.id: l.name for l in self.network.links}
        }

        try:

            # write metadata
            with open('{}/metadata.json'.format(base_path), 'w') as f:
                json.dump(self.metadata, f, sort_keys=True, indent=4, separators=(',', ': '))

            count = 1
            pcount = 1
            nparams = len(self.store)
            path = base_path + '/data/{parameter}.csv'
            results = {}
            for key, values in self.store.items():
                pcount += 1

                res_type, res_id, attr_id = key.split('/')
                res_id = int(res_id)
                attr_id = int(attr_id)

                tattr = self.conn.tattrs.get((res_type, res_id, attr_id))
                if not tattr:
                    # Same as previous issue.
                    # This is because the model assigns all resource attribute possibilities to all resources of like type
                    # In practice this shouldn't make a difference, but may result in a model larger than desired
                    # TODO: correct this
                    continue
                param_name = get_param_name(res_type, tattr['attr_name'])

                if param_name not in self.params:
                    continue  # it's probably an internal variable/parameter

                res_name = res_names.get(res_type, {}).get(res_id) or self.network.name
                try:
                    data = pd.DataFrame.from_dict(values, orient='index', columns=[res_name])
                    if param_name not in results:
                        results[param_name] = data
                    else:
                        results[param_name] = pd.concat([results[param_name], data], axis=1, sort=True)
                except:
                    continue

            for param_name, data in results.items():

                if not data.empty:
                    data.to_csv('{}/{}.csv'.format(base_path, param_name))

                if count % 10 == 0 or pcount == nparams:
                    if self.scenario.reporter:
                        self.scenario.reporter.report(action='save',
                                                      saved=round(count / (self.nparams + self.nvars) * 100))
                count += 1

        except:
            msg = 'ERROR: Results could not be saved.'
            # self.logd.info(msg)
            if self.scenario.reporter:
                self.scenario.reporter.report(action='error', message=msg)
            raise
