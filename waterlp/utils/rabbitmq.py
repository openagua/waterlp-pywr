import requests


class API(object):
    def __init__(self, url, path, auth):
        self.auth = auth
        self.path = url + path

    def get(self, **urlparts):
        url = self.path.format(**urlparts)
        return requests.get(url, auth=self.auth)

    def put(self, json, **urlparts):
        url = self.path.format(**urlparts)
        return requests.put(url, auth=self.auth, json=json)

    def delete(self, **urlparts):
        url = self.path.format(**urlparts)
        return requests.delete(url, auth=self.auth)


class RabbitMQ(object):

    def __init__(self, api_url=None, host=None, username=None, password=None):
        if not api_url and host:
            api_url = 'http://{host}:15672/api'.format(host=host)
        auth = (username, password)

        self.vhost = 'model-run'
        self.VHosts = API(url=api_url, path='/vhosts/{vhost}', auth=auth)
        self.Users = API(url=api_url, path='/users/{user}', auth=auth)
        self.Permissions = API(url=api_url, path='/permissions/{vhost}/{user}', auth=auth)
        self.Queues = API(url=api_url, path='/queues/{vhost}/{{name}}'.format(vhost=self.vhost), auth=auth)

    def update_rabbitmq_user(self, old_key, new_key, model_name):
        # delete old vhosts & user
        # resp = self.VHosts.delete(user=old_key)
        resp = self.Users.delete(user=old_key)

        # add new vhosts & user
        # resp = self.VHosts.put({'tracing': False}, vhost=new_key)
        resp = self.Users.put({'password': 'password', 'tags': model_name}, user=new_key)

        # add user permissions
        # resp = self.Permissions.put({"configure": ".*", "write": ".*", "read": ".*"}, vhost=new_key, user=new_key)
        resp = self.Permissions.put({"configure": ".*", "write": ".*", "read": ".*"}, vhost=self.vhost, user=new_key)
        return

    def get_queue(self, queue_name):
        return self.Queues.get(name=queue_name)

    def add_queue(self, json, queue_name):
        return self.Queues.put(json, name=queue_name)
