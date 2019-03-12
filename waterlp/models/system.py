import os
import json
from attrdict import AttrDict
import pandas as pd
import boto3
import pendulum

from waterlp.models.pywr import PywrModel
from waterlp.models.evaluator import Evaluator
from waterlp.utils.converter import convert

INITIAL_STORAGE_ATTRS = [
    ('Reservoir', 'Initial Storage'),
    ('Groundwater', 'Initial Storage')
]


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


def add_subblocks(self, values, attr_name):
    subblocks = self.default_subblocks
    nsubblocks = self.nsubblocks

    new_values = {}

    if attr_name in self.demandParams:
        new_vals = {}
        try:
            for block in values:
                for d, v in values[block].items():
                    new_vals[d] = v / nsubblocks
                for i, subblock in enumerate(subblocks):
                    new_values[(block, subblock)] = new_vals
        except:
            raise

    elif attr_name in self.valueParams:
        try:
            for block in values:
                for i, subblock in enumerate(subblocks):
                    new_vals = {}
                    for d, v in values[block].items():
                        # new_vals[d] = v + (1 - sqrt((nsubblocks - i) / nsubblocks))
                        new_vals[d] = v - 1 + ((nsubblocks - i) / nsubblocks) ** 2
                    new_values[(block, subblock)] = new_vals
        except:
            raise

    return new_values


class WaterSystem(object):

    def __init__(self, conn, name, network, all_scenarios, template, args, date_format='iso',
                 session=None, reporter=None, scenario=None):

        self.storage = network.layout.get('storage')
        self.bucket_name = args.aws_s3_bucket

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
        self.initial_volumes = {}  # assume these are only for nodes

        self.scenarios = {s.name: s for s in all_scenarios}
        self.scenarios_by_id = {s.id: s for s in all_scenarios}

        self.foresight = args.foresight  # pending further development

        # extract info about nodes & links
        self.network = network
        self.resources = {}
        self.ttypes = {}
        self.res_tattrs = {}

        self.constants = {}  # fixed (scalars, arrays, etc.)
        self.variables = {}  # variable (time series)
        self.initial_conditions = {}
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
        rtypeattrs = {}

        for tt in template.types:
            resource_type = tt.resource_type.lower()  # i.e., node, link, network
            self.ttypes[(resource_type, tt['name'])] = []

            # use typeattrs to track variables/parameters
            ttypeattrs[(resource_type, tt['name'])] = tt.typeattrs

        # organize basic network information
        # features['networks'] = [network]

        def get_resource_attributes(resource, resource_type):
            rtypes = [rt for rt in resource.types if rt.template_id == template.id]
            if not rtypes:
                return
            elif resource_type != 'network' and len(rtypes) > 1:
                raise Exception("More than one type for {} {}".format(resource_type, resource['name']))
            else:
                rt = rtypes[0]  # network type should be the first one

            idx = (resource_type, resource.id)
            resource['type'] = rt
            self.resources[idx] = resource

            # a dictionary of template_type to resource
            self.ttypes[(resource_type, rt['name'])].append(idx)
            # self.res_ttypes[idx] = rt['name']

            tattrs = {ta.attr_id: ta for ta in ttypeattrs[(resource_type, rt['name'])]}

            res_tattrs = list(tattrs.keys())

            # general resource attribute information
            for ra in resource.attributes:
                if ra.attr_id not in res_tattrs:
                    continue
                self.res_tattrs[ra.id] = tattrs[ra.attr_id]

                if ra.attr_is_var == 'N' and not args.suppress_input:
                    self.nparams += 1
                else:
                    self.nvars += 1

        get_resource_attributes(network, 'network')
        for node in network.nodes:
            get_resource_attributes(node, 'node')
        for link in network.links:
            get_resource_attributes(link, 'link')

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
        if resource_type == 'network':
            resource_name = self.network['name']
        else:
            resource_name = self.resources.get((resource_type, resource_id), {}).get('name', 'unknown resource')

        msg = 'Error calculating {attr} at {rtype} {res}:\n\n{exc}'.format(
            attr=attr_name,
            rtype=resource_type,
            res=resource_name,
            exc=message
        )

        print(msg)

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

            settings['network_files_path'] = self.bucket_name and network_folder and 's3://{}/{}'.format(
                self.bucket_name,
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

                if rs.resource_attr_id not in self.res_tattrs:
                    continue  # this is for a different resource type

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
                elif rs.resource_attr_id in self.ra_link:
                    resource_type = 'link'
                    resource_id = self.ra_link[rs.resource_attr_id]
                else:
                    resource_type = 'network'
                    resource_id = self.network.id
                res_idx = (resource_type, resource_id)

                try:

                    res_tattr = self.res_tattrs.get(rs.resource_attr_id)
                    if not res_tattr:
                        continue  # this is for a different resource type

                    # get attr name
                    attr_id = res_tattr['attr_id']
                    tattr = self.conn.tattrs[(resource_type, resource_id, attr_id)]
                    if not tattr:
                        continue
                    intermediary = tattr['properties'].get('intermediary', False)
                    # attr_name = tattr['att']
                    is_var = tattr['is_var'] == 'Y'

                    # non-intermediary outputs should not be pre-processed at all
                    if is_var and not intermediary:
                        continue

                    # create a dictionary to lookup resourcescenario by resource attribute ID
                    self.res_scens[rs.resource_attr_id] = rs

                    # load the metadata
                    metadata = json.loads(rs.value.metadata)

                    # identify as function or not
                    is_function = metadata.get('use_function', 'N') == 'Y'

                    # get data type
                    data_type = rs.value.type

                    # update data type
                    self.res_tattrs[rs.resource_attr_id]['data_type'] = data_type

                    # default blocks
                    # NB: self.block_params should be defined
                    # TODO: update has_blocks from template, not metadata
                    # has_blocks = attr_name in self.block_params or metadata.get('has_blocks', 'N') == 'Y'
                    has_blocks = False
                    blocks = [(0, 0)]

                    type_name = self.resources[(resource_type, resource_id)]['type']['name']
                    tattr_idx = (resource_type, type_name, attr_id)

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
                                fill_value=0,
                                has_blocks=has_blocks,
                                date_format=self.date_format,
                                flavor='native',
                                parentkey=parentkey
                            )
                    except:
                        raise

                    if not is_var and (value is None or (type(value) == str and not value)):
                        continue

                    # TODO: add generic unit conversion utility here
                    dimension = rs.value.dimension

                    if data_type == 'scalar':
                        try:
                            value = float(value)
                        except:
                            raise Exception("Could not convert scalar")

                        if (type_name, tattr['attr_name']) in INITIAL_STORAGE_ATTRS:
                            if tattr_idx not in self.initial_volumes:
                                self.initial_volumes[tattr_idx] = {}
                            self.initial_volumes[tattr_idx][resource_id] = value

                        else:
                            if tattr_idx not in self.constants:
                                self.constants[tattr_idx] = {}
                            self.constants[tattr_idx][res_idx] = value


                    elif data_type == 'descriptor':  # this could change later
                        if tattr_idx not in self.constants:
                            self.constants[tattr_idx] = {}
                        self.constants[tattr_idx][res_idx] = value

                    elif data_type == 'timeseries':
                        values = value
                        function = None

                        try:
                            if is_function:
                                function = metadata['function']
                                if not function:  # if there is no function, this will be treated as no dataset
                                    continue

                            # routine to add blocks using quadratic values - this needs to be paired with a similar routine when updating boundary conditions
                            # if has_blocks:
                            #     values = add_subblocks(values, attr_name, self.default_subblocks)

                            if tattr_idx not in self.variables:
                                self.variables[tattr_idx] = {}

                            self.variables[tattr_idx][res_idx] = {
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
                    if res_idx in type_blocks:
                        blocks = blocks if len(blocks) > len(type_blocks[res_idx]) else type_blocks[res_idx]
                    self.blocks[resource_type][res_idx] = blocks

                except Exception as err:
                    if resource_type == 'network':
                        resource_name = 'network'
                    else:
                        resource_name = self.resources.get((resource_type, resource_id), {}).get('name',
                                                                                                 'unknown resource')

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

        current_dates = self.dates[:self.foresight_periods]
        current_dates_as_string = self.dates_as_string[:self.foresight_periods]
        step = self.dates[0].day

        # set up the time steps
        start = current_dates_as_string[0]
        end = current_dates_as_string[-1]
        step = step

        # set up the initial volumes
        initial_volumes = {}
        for tattr_idx, values in self.initial_volumes.items():
            scale = self.params[tattr_idx]['scale']
            unit = self.params[tattr_idx]['unit']
            for resource_id, value in values.items():
                initial_volumes[resource_id] = convert(value * scale, 'Volume', unit, 'hm^3')

        self.model = PywrModel(
            network=self.network,
            template=self.template,
            start=start,
            end=end,
            step=step,
            initial_volumes=initial_volumes
        )

    def prepare_params(self):
        """
        Declare parameters, based on the template type.
        The result is a dictionary of all parameters for later use and extension.
        """

        for ttype in self.template.types:

            resource_type = ttype['resource_type']

            # if resource_type == 'NETWORK':
            #     continue

            for tattr in ttype.typeattrs:

                # data_type = tattr['data_type']

                # create a unique parameter index
                attr_id = tattr['attr_id']
                type_name = ttype['name']
                tattr_idx = (resource_type.lower(), type_name, attr_id)
                if tattr_idx not in self.params:
                    param = AttrDict(tattr)
                    param.update(param.properties)
                    param.update(
                        scale=param.get('scale', 1),
                        unit=param.get('unit'),
                        intermediary=param.get('intermediary', False),
                        has_blocks=param.get('has_blocks', False),
                        resource_type=resource_type.lower()
                    )
                    del param['properties']
                    self.params[tattr_idx] = param

                    if tattr['attr_name'] == 'Initial Storage':
                        self.storage_scale = param.get('scale', 1)
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

                attr_id = tattr['attr_id']
                type_name = self.resources[(resource_type, resource_id)]['type']['name']
                tattr_idx = (resource_type, type_name, attr_id)

                idx = (resource_type, resource_id)

                # at this point, timeseries have not been assigned to variables, so these are mutually exclusive
                # the order here shouldn't matter
                variable = self.constants.get(tattr_idx, {}).get(idx)
                timeseries = self.variables.get(tattr_idx, {}).get(idx)
                if variable:
                    self.constants[tattr_idx][idx] = perturb(self.constants[tattr_idx][idx], variation)

                elif timeseries:
                    if not timeseries.get('function'):  # functions will be handled by the evaluator
                        self.variables[tattr_idx][idx]['values'] = perturb(self.variables[tattr_idx][idx]['values'],
                                                                           variation)

                else:  # we need to add the variable to account for the variation
                    data_type = tattr['data_type']
                    if data_type == 'scalar':
                        if tattr_idx not in self.constants:
                            self.constants[tattr_idx] = {}
                        self.constants[tattr_idx][idx] = perturb(0, variation)
                    elif data_type == 'timeseries':

                        if tattr_idx not in self.constants:
                            self.constants[tattr_idx] = {}
                        self.variables[tattr_idx][idx] = {
                            'values': perturb(self.evaluator.default_timeseries.copy(), variation),
                            'dimension': tattr['dimension']
                        }

    def update_boundary_condition(self, res_idx, tattr_idx, dates_as_string, is_function=False, func=None, values=None,
                                  step='main', scope='store'):

        try:
            resource_type, type_name, attr_id = tattr_idx
            resource_type, resource_id = res_idx
            param = self.params[tattr_idx]
            if scope == 'store' \
                    and (step == 'main' and param.intermediary
                         or step in ['pre-process', 'post-process'] and not param.intermediary):
                return

            if scope == 'model' and param.intermediary:
                return

            if param.is_var == 'Y' and step != 'post-process':
                return

            dimension = param.dimension
            data_type = param.data_type
            unit = param.unit
            startup_date = self.constants.get('Startup Date', '')

            # for updating Pywr
            type_name_lower = type_name.lower()
            attr_name_lower = param['attr_name'].lower()

            parentkey = '{}/{}/{}'.format(resource_type, resource_id, attr_id)

            if is_function:
                if scope == 'store':
                    self.evaluator.data_type = data_type
                    try:
                        # full_key = (resource_type, resource_id, attr_id, dates_as_string)
                        values = self.evaluator.eval_function(
                            func,
                            has_blocks=param.has_blocks,
                            flatten=not param.has_blocks,
                            data_type=data_type,
                            parentkey=parentkey,
                            flavor='native'
                        )
                    except Exception as err:
                        raise self.create_exception(parentkey, str(err))

                else:
                    values = self.get_value(resource_type, resource_id, attr_id, has_blocks=param.has_blocks)

                # update missing blocks, if any
                # routine to add blocks using quadratic values - this needs to be paired with a similar routine when updating boundary conditions
                # if has_blocks:
                #     values = add_subblocks(values, attr_name, self.default_subblocks)

            if param.has_blocks:
                cols = values.keys()
            else:
                cols = [0]
            for j, c in enumerate(cols):

                if param.has_blocks:
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

                    if scope == 'store':
                        # send the result to the data store
                        self.store_value(resource_type, resource_id, attr_id, datetime, val,
                                         has_blocks=param.has_blocks)
                        continue

                    # if step != 'main':
                    #     continue

                    if val is not None:
                        scale = param.scale
                        # only convert if updating the LP model
                        if dimension == 'Volumetric flow rate':
                            val = convert(val * scale, dimension, unit, 'hm^3 day^-1')
                        elif dimension == 'Volume':
                            val = convert(val * scale, dimension, unit, 'hm^3')

                    try:
                        self.model.update_param(resource_type, resource_id, type_name_lower, attr_name_lower, val)

                    except Exception as err:
                        print(err)
                        raise
        except Exception as err:
            print(err)
            raise

    def step(self):
        self.model.model.step()

    def run(self):
        self.model.model.run()

    def finish(self):
        self.save_results()
        self.model.model.finish()

    def update_boundary_conditions(self, tsi, tsf, step='main', initialize=False):
        """
        Update boundary conditions.
        """
        dates_as_string = self.dates_as_string[tsi:tsf]
        self.evaluator.tsi = tsi
        self.evaluator.tsf = tsf

        # 1. Update values in memory store
        for tattr_idx, params in self.variables.items():
            for res_idx, param in params.items():
                self.update_boundary_condition(
                    res_idx,
                    tattr_idx,
                    dates_as_string,
                    values=param.get('values'),
                    is_function=param.get('is_function'),
                    func=param.get('function'),
                    step=step,
                    scope='store'
                )

        # 2. update Pyomo model
        if step == 'main':
            # for attr_name in self.valueParams + self.demandParams:
            self.model.updated = {}
            for tattr_idx, params in self.variables.items():
                for res_idx, param in params.items():
                    self.update_boundary_condition(
                        res_idx,
                        tattr_idx,
                        dates_as_string,
                        values=param.get('values'),
                        is_function=param.get('is_function'),
                        func=param.get('function'),
                        step=step,
                        scope='model'
                    )

    def collect_results(self, timesteps, tsidx, include_all=False, suppress_input=False):

        # loop through all the model parameters and variables
        for (resource_type, resource_id), node in self.model.non_storage.items():
            self.store_results(
                resource_type=resource_type,
                resource_id=resource_id,
                attr_name='inflow',
                timestamp=timesteps[0],
                value=node.flow[0],
            )

            self.store_results(
                resource_type=resource_type,
                resource_id=resource_id,
                attr_name='outflow',
                timestamp=timesteps[0],
                value=node.flow[0],
            )

        for resource_id, node in self.model.storage.items():
            self.store_results(
                resource_type='node',
                resource_id=resource_id,
                attr_name='storage',
                timestamp=timesteps[0],
                value=node.volume[0],
            )
            self.store_results(
                resource_type='node',
                resource_id=resource_id,
                attr_name='outflow',
                timestamp=timesteps[0],
                value=sum([input.flow[0] for input in node.inputs]),  # "input" means "input to the system"
            )
            self.store_results(
                resource_type='node',
                resource_id=resource_id,
                attr_name='inflow',
                timestamp=timesteps[0],
                value=sum([output.flow[0] for output in node.outputs]),
            )

    def store_results(self, resource_type=None, resource_id=None, attr_name=None, timestamp=None, value=None):

        type_name = self.resources[(resource_type, resource_id)]['type']['name']
        attr_id = self.conn.attr_id_lookup.get((resource_type, resource_id, attr_name))
        if not attr_id:
            return  # this is not an actual attribute in the model
        tattr_idx = (resource_type, type_name, attr_id)
        param = self.params.get(tattr_idx, {})

        has_blocks = param.has_blocks
        dimension = param.dimension
        unit = param.unit
        scale = param.scale

        # collect to results

        # the purpose of this addition is to aggregate blocks, if any, thus eliminating the need for Pandas
        # on the other hand, it should be checked which is faster: Pandas group_by or simple addition here

        if dimension == 'Volume':
            value = convert(value, dimension, 'hm^3', unit) / scale
        elif dimension == 'Volumetric flow rate':
            value = convert(value, dimension, 'hm^3 day^-1', unit) / scale

        # store in evaluator store
        self.store_value(resource_type, resource_id, attr_id, timestamp, value, has_blocks=has_blocks)

    def get_value(self, resource_type, resource_id, attr_id, timestamp=None, has_blocks=False):

        key_string = '{resource_type}/{resource_id}/{attr_id}'.format(resource_type=resource_type,
                                                                      resource_id=resource_id, attr_id=attr_id)
        if has_blocks:
            val = self.store[key_string]  # TODO: get specific block
        else:
            val = self.store[key_string]
        if timestamp:
            return val[timestamp]
        else:
            return val

    def store_value(self, resource_type, resource_id, attr_id, timestamp, val, has_blocks=False):

        # add new resource scenario if it doesn't exist
        key = (resource_type, resource_id, attr_id)
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
        key_string = '{resource_type}/{resource_id}/{attr_id}'.format(resource_type=resource_type,
                                                                      resource_id=resource_id, attr_id=attr_id)
        if key_string not in self.store:
            if has_blocks:
                self.store[key_string] = {0: {}}
            else:
                self.store[key_string] = {}
        elif has_blocks and 0 not in self.store[key_string]:
            self.store[key_string][0] = {}
        if has_blocks:
            # val += self.store[key_string][0].get(timestamp, 0)
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

    def save_to_file(self, filename, content):
        s3 = boto3.client('s3')
        key = '{network_folder}/{log_dir}/{filename}'.format(
            network_folder=self.storage.folder,
            log_dir=self.log_dir,
            filename=filename
        )
        s3.put_object(Body=content, Bucket=self.bucket_name, Key=key)

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
        # mod_date = mktime(dt.utcnow().timetuple())
        mod_date = pendulum.now().to_iso8601_string()
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

            result = self.conn.call('update_scenario', {
                'scen': result_scenario,
            })

        self.scenario.scenario_id = result_scenario['id']

        # save variable data to database
        res_scens = []
        mb = 0
        res_names = {}

        try:
            n = 0
            N = len(self.store)
            for key, value in self.store.items():
                n += 1
                resource_type, resource_id, attr_id = key.split('/')
                resource_id = int(resource_id)
                attr_id = int(attr_id)

                tattr = self.conn.tattrs.get((resource_type, resource_id, attr_id))
                if not tattr:
                    continue

                type_name = self.resources[(resource_type, resource_id)]['type']['name']
                param_idx = (resource_type, type_name, attr_id)
                param = self.params.get(param_idx)
                if not param:
                    continue  # it's probably an internal variable/parameter

                # create the resource scenario (dataset attached to a specific resource attribute)
                res_idx = (resource_type, resource_id, attr_id)
                res_attr_id = self.conn.res_attr_lookup.get(res_idx)
                if not res_attr_id:
                    continue
                resource_name = self.conn.raid_to_res_name[res_attr_id]
                attr_name = tattr['attr_name']

                # define the dataset value
                try:
                    if param.has_blocks and type(list(value.values())[0]) == dict:
                        value = pd.DataFrame(value).to_json()
                    else:
                        value = pd.DataFrame({0: value}).to_json()
                except:
                    print('Failed to prepare: {}'.format(attr_name))
                    continue

                # if self.args.debug:
                #     print('Saving: {} for {}'.format(attr_name, resource_name))

                if resource_type == 'network':
                    res_scen_name = '{} - {} [{}]'.format(self.network.name, tattr['attr_name'], self.scenario.name)
                else:
                    res_scen_name = '{} - {} - {} [{}]'.format(self.network.name,
                                                               resource_name,
                                                               attr_name,
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

            self.scenario.result_scenario_id = result_scenario['id']

            if self.scenario.reporter:
                if N:
                    self.scenario.reporter.report(action='save', saved=round(n / N * 100))
                else:
                    self.scenario.reporter.report(action='error',
                                                  message="ERROR: No results have been reported. The model might not have run.")

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
    #         for attr_name, param_values in self.store.items():
    #             pcount += 1
    #             df = self.param_to_df(attr_name, param_values)
    #             content = df.to_csv().encode()
    #
    #             if content:
    #                 s3.put_object(Body=content, Bucket='openagua.org', Key=path.format(parameter=attr_name))
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

                resource_type, resource_id, attr_id = key.split('/')
                resource_id = int(resource_id)
                attr_id = int(attr_id)

                tattr = self.conn.tattrs.get((resource_type, resource_id, attr_id))
                if not tattr:
                    # Same as previous issue.
                    # This is because the model assigns all resource attribute possibilities to all resources of like type
                    # In practice this shouldn't make a difference, but may result in a model larger than desired
                    # TODO: correct this
                    continue

                type_name = self.resources[(resource_type, resource_id)]['type']['name']
                attr_name = tattr['attr_name']
                tattr_idx = (resource_type, type_name, attr_name)

                if tattr_idx not in self.params:
                    continue  # it's probably an internal variable/parameter

                res_name = res_names.get(resource_type, {}).get(resource_id) or self.network.name
                try:
                    data = pd.DataFrame.from_dict(values, orient='index', columns=[res_name])
                    if tattr_idx not in results:
                        results[tattr_idx] = data
                    else:
                        results[tattr_idx] = pd.concat([results[tattr_idx], data], axis=1, sort=True)
                except:
                    continue

            for (resource_type, type_name, attr_name), data in results.items():

                if not data.empty:
                    data.to_csv('{}/{}.csv'.format(base_path, attr_name))

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
