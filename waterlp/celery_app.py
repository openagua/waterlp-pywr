import getpass
from os import path, environ, makedirs
from shutil import rmtree
from celery import Celery
from kombu import Connection, Queue, Exchange

from waterlp.utils.application import PNSubscribeCallback

from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub

run_key = environ.get('RUN_KEY')
model_key = environ['MODEL_KEY']
queue_name = 'model-{}'.format(model_key)
if run_key:
    queue_name += '-{}'.format(run_key)

broker_url = 'amqp://{username}:{password}@{hostname}/{vhost}'.format(
    username=model_key,
    password=environ.get('RABBITMQ_PASSWORD', 'password'),
    hostname=environ.get('RABBITMQ_HOST', 'localhost'),
    vhost=environ.get('RABBITMQ_VHOST', 'model-run'),
)

app = Celery(
    'tasks',
    broker=broker_url,
    include=['waterlp.tasks'],
    task_routes={'waterlp.tasks': {'queue': queue_name}}
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

    pubnub.subscribe().channels(queue_name).execute()
    print(" [*] Subscribed to PubNub")

    hostname = environ.get('RABBITMQ_HOST', 'localhost')
    run_key = environ.get('RUN_KEY')
    vhost = environ.get('RABBITMQ_VHOST', 'model-run')
    userid = environ.get('RABBITMQ_USERNAME', model_key)
    password = environ.get('RABBITMQ_PASSWORD', 'password')

    url = 'amqp://{username}:{password}@{hostname}/{vhost}'.format(
        username=userid,
        password=password,
        hostname=hostname,
        vhost=vhost,
    )

    with Connection(url) as conn:
        with conn.channel() as channel:
            task_queue = Queue(
                name=queue_name,
                durable=True,
                auto_delete=False,
                message_ttl=3600,
            )
            task_queue.declare(True, channel)
            print(" [*] Task queue created")

    try:

        print(" [*] Starting Celery")
        app.start(['celery', 'worker', '-l', 'ERROR'])

    except KeyboardInterrupt:
        pubnub.unsubscribe_all()
        print('Stopped by user.')
