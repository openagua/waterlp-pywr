import getpass
from os import path, environ, makedirs
from shutil import rmtree
from celery import Celery

run_key = environ.get('RUN_KEY')
model_key = environ.get('MODEL_KEY')
queue_name = 'model-{}'.format(model_key)
if run_key:
    queue_name += '-{}'.format(run_key)

broker_url = 'pyamqp://{username}:{password}@{hostname}:5672/{vhost}'.format(
    username=model_key,
    password=environ.get('RABBITMQ_PASSWORD', 'password'),
    hostname=environ.get('RABBITMQ_HOST', 'localhost'),
    vhost=environ.get('RABBITMQ_VHOST', 'model-run'),
)

app = Celery('openagua', broker=broker_url, include=['waterlp.tasks'])

app.conf.update(
    task_default_queue=queue_name,
    task_default_exchange='openagua.run',
    broker_heartbeat=10,
    accept_content=['json', 'pickle'],
    result_expires=3600,
    worker_prefetch_multiplier=1,
)


def start_listening(concurrency=4):
    from waterlp.utils.application import PNSubscribeCallback

    from pubnub.pnconfiguration import PNConfiguration
    from pubnub.pubnub import PubNub

    # app.config_from_object('waterlp.celeryconfig')
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

    app.start(['celery', 'worker', '-c', str(concurrency), '-l', 'INFO'])


if __name__ == '__main__':
    start_listening()
