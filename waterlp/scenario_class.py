from waterlp.utils.scenarios import create_subscenarios
from datetime import datetime as dt

statuses = {
    'start': 'started',
    'done': 'finished',
    'error': 'error',
    'pause': 'paused',
    'resume': 'resuming',
    'stop': 'stopped',
    'step': 'running',
    'save': 'saving'
}


class Scenario(object):
    def __init__(self, scenario_ids, conn, network, template, args, scenario_lookup):
        self.base_scenarios = []
        self.source_id = args.source_id
        self.run_name = args.run_name
        self.source_scenarios = {}
        self.network_id = network.id
        self.reporter = None
        self.total_steps = 1
        self.finished = 0
        self.current_date = None
        self.scenario_id = None  # result scenario ID

        # self.start_time = '0'
        # self.end_time = '9'
        self.start_time = None
        self.end_time = None
        self.time_step = ''

        # look for existing option-scenario combination
        source_names = []
        tags = []

        loaded_scenarios = {s.id: s for s in network.scenarios}

        # collect source IDs
        self.source_ids = []
        if scenario_ids[1] == scenario_ids[0]:
            scenario_ids.pop()

        self.scenario_ids = scenario_ids
        self.unique_id = args.unique_id + '-' + '-'.join(str(s_id) for s_id in scenario_ids)

        for i, base_id in enumerate(scenario_ids):

            source = [s for s in network.scenarios if s.id == base_id][0]

            if i and source.id in self.source_ids:
                continue  # this is a baseline scenario; already accounted for

            self.base_scenarios.append(source)
            self.source_scenarios[base_id] = source

            this_chain = [source.id]

            # TODO: pull this chaining info from list of scenarios rather than hitting Hydra Platform multiple times
            while source['layout'].get('parent'):
                parent_id = source['layout']['parent']
                if parent_id not in self.source_ids:  # prevent adding in Baseline twice, which would overwrite options
                    this_chain.append(parent_id)
                if parent_id in loaded_scenarios:
                    source = loaded_scenarios[parent_id]
                else:
                    source = conn.call('get_scenario', {'scenario_id': parent_id})

                self.source_scenarios[source.id] = source

            # source should not have a parent at this point, so this should be for the baseline scenario
            if i == 0:
                self.time_step = source.get('time_step')

            this_chain.reverse()

            self.source_ids.extend(this_chain)  # include baseline

        self.base_ids = []
        for s in self.base_scenarios:
            self.base_ids.append(s.id)
            tags.extend(s.layout.get('value_tags', []))

            source_names.append(s.name)

        self.source_name = ' - '.join(source_names)
        if self.run_name:
            self.name = '{}: {}'.format(self.run_name, self.source_name)

        # mod_date = datetime.now().isoformat()
        # results_scenario_name = '{} - {}'.format(self.name, mod_date)
        results_scenario_name = self.name

        self.option = self.base_scenarios[0]
        self.scenario = self.base_scenarios[-1]

        for i, source in enumerate(self.source_scenarios.values()):
            self.start_time = source.get('start_time', self.start_time)
            self.end_time = source.get('end_time', self.end_time)

            # self.start_time = max(self.start_time, source.get('start_time', '0000'))
            # self.end_time = min(self.end_time, source.get('end_time', '9999'))

        # ######################
        # Create sub scenarios
        # ######################

        self.subscenarios = {
            'options':  create_subscenarios(network, template, self.option, 'option'),
            'scenarios': create_subscenarios(network, template, self.scenario, 'scenario'),
        }

        self.variation_count = 0
        for sss in self.subscenarios.values():
            for ss in sss:
                if ss['variations']:
                    self.variation_count += 1

        # ######################
        # Create result scenario
        # ######################

        result_scenario = scenario_lookup.get(results_scenario_name)
        if not result_scenario or result_scenario.id in self.source_ids:
            result_scenario = conn.call(
                'add_scenario',
                {
                    'network_id': network.id,
                    'scen': {
                        'id': None,
                        'name': results_scenario_name,
                        # 'cr_date': mod_date,
                        'description': '',
                        'network_id': network.id,
                        'layout': {
                            'class': 'results',
                            'sources': self.base_ids,
                            'value_tags': tags,
                            'run': args.run_name,
                        }
                    }
                }
            )

        # where should results be saved?
        if self.variation_count == 0:
            self.destination = args.destination
        else:
            self.destination = 's3'

        self.version_date = args.starttime.strftime('%Y-%m-%d %H:%M:%S')

        # update the result scenario
        versions = result_scenario['layout'].get('versions', [])
        versions.append({
            'number': len(versions) + 1,
            'date': self.version_date,
            'variations': self.variation_count,
            'human_readable': not args.human_readable
        })
        result_scenario['layout'].update({
            'data_location': self.destination,
            'versions': versions,
            'modified_date': self.version_date,
            'value_tags': tags,
        })

        result = conn.call('update_scenario', {'scen': result_scenario})

        # write variation info to s3

        self.storage = network.layout.get('storage')

        if self.destination == 's3':
            self.base_path = '{folder}/.results/{run}/{date}/{scenario}'.format(
                folder=self.storage.folder,
                run=self.run_name,
                scenario=result_scenario.name if args.human_readable else result_scenario.id,
                date=self.version_date,
            )

        self.result_scenario = result_scenario

    def update_payload(self, action=None, **payload):
        payload.update({
            'sid': self.unique_id,
            'name': self.run_name,
            'source_id': self.source_id,
            'network_id': self.network_id,
            'scids': self.scenario_ids,
            # 'scenario_name': self.name,
            'scenario_id': self.scenario_id,
            'status': 'unknown'
        })
        if action:
            payload.update({
                'action': action,
                'status': statuses.get(action, 'unknown'),
                'date': self.current_date,
                'progress': int(round(self.finished / self.total_steps * 100)),
            })
            if action == 'start':
                payload.update({
                    # 'progress': 0,
                })
            elif action == 'step':
                payload.update({

                })
            elif action == 'save':
                payload.update({
                    # 'progress': self.finished / self.total_steps * 100,
                })
            elif action == 'done':
                payload.update({
                    'progress': 100,
                    'saved': 100,
                })
        return payload
