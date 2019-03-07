import getpass
from os import path, environ, makedirs
from shutil import rmtree
from celery import Celery
from kombu import Queue

from waterlp.utils.application import PNSubscribeCallback

from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub

run_key = environ.get('RUN_KEY')
model_key = 'model-{}'.format(environ['MODEL_KEY'])
if run_key:
    model_key += '-{}'.format(run_key)

broker_url = 'amqp://{username}:{password}@{hostname}/{vhost}'.format(
    username=environ['MODEL_KEY'],
    password=environ.get('RABBITMQ_PASSWORD', 'password'),
    hostname=environ.get('RABBITMQ_HOST', 'localhost'),
    vhost=environ.get('RABBITMQ_VHOST', 'model-run'),
)

app = Celery(
    'tasks',
    broker=broker_url,
    include=['waterlp.tasks'],
    # task_queues=[Queue('tasks', routing_key=model_key)]
)

app.config_from_object('waterlp.celeryconfig')

if __name__ == '__main__':

    # TODO: migrate listen/Kombu to here (100% Celery)
    # see: https://www.distributedpython.com/2018/06/19/call-celery-task-outside-codebase/

    app_dir = '/home/{}/.waterlp'.format(getpass.getuser())
    logs_dir = '{}/logs'.format(app_dir)
    if path.exists(app_dir):
        rmtree(app_dir)
    makedirs(logs_dir)

    pnconfig = PNConfiguration()
    pnconfig.subscribe_key = environ.get('PUBNUB_SUBSCRIBE_KEY')
    pnconfig.ssl = False
    pubnub = PubNub(pnconfig)
    pubnub.add_listener(PNSubscribeCallback())

    pubnub.subscribe().channels(model_key).execute()
    print(" [*] Subscribed to PubNub")

    try:

        # app.start(['celery', '-A', 'waterlp.celery_app', 'worker', '-l', 'info'])
        app.start(['celery', 'worker', '-l', 'ERROR'])
        print(" [*] Celery app started")


    except KeyboardInterrupt:
        pubnub.unsubscribe_all()
        print('Stopped by user.')
