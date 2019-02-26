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
    def __init__(self, scenario_ids, conn, network, args):
        self.base_scenarios = []
        self.source_id = args.source_id
        self.run_name = args.run_name
        self.source_scenarios = {}
        self.network_id = network.id
        self.reporter = None
        self.total_steps = 1
        self.finished = 0
        self.current_date = None

        # self.start_time = '0'
        # self.end_time = '9'
        self.start_time = None
        self.end_time = None
        self.time_step = ''

        # look for existing option-scenario combination
        source_names = []
        self.tags = []

        loaded_scenarios = {s.id: s for s in network.scenarios}

        # collect source IDs
        self.source_ids = []
        if scenario_ids[1] == scenario_ids[0]:
            scenario_ids.pop()

        self.scenario_ids = scenario_ids
        self.unique_id = args.unique_id + '-' + '-'.join(str(s_id) for s_id in scenario_ids)

        for i, base_id in enumerate(scenario_ids):
            # if i and source.id in self.source_ids:
            # continue # this is a baseline scenario; already accounted for

            source = [s for s in network.scenarios if s.id == base_id][0]
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

            if i:
                this_chain = this_chain[1:]

            self.source_ids.extend(this_chain)  # include baseline

        self.base_ids = []
        for s in self.base_scenarios:
            self.base_ids.append(s.id)
            if s.layout.get('tags'):
                self.tags.extend(s.layout.tags)

            source_names.append(s.name)

        self.name = ' - '.join(source_names)
        if args.run_name:
            self.name = '{} - {}'.format(args.run_name, self.name)
        # results_scenario_name = '{}; {}'.format(base_name, self.starttime.strftime('%Y-%m-%d %H:%M:%S'))

        self.option = self.base_scenarios[0]
        self.scenario = self.base_scenarios[-1]

        for i, source in enumerate(self.source_scenarios.values()):
            self.start_time = source.get('start_time', self.start_time)
            self.end_time = source.get('end_time', self.end_time)

            # self.start_time = max(self.start_time, source.get('start_time', '0000'))
            # self.end_time = min(self.end_time, source.get('end_time', '9999'))

    def update_payload(self, action=None, **payload):
        payload.update({
            'sid': self.unique_id,
            'name': self.run_name,
            'source_id': self.source_id,
            'network_id': self.network_id,
            'scids': self.scenario_ids,
            # 'scenario_name': self.name,
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
                    # 'progress': self.finished / self.total_steps * 100,
                    'saved': 100,
                })
        return payload
