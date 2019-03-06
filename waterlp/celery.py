from os import environ
from celery import Celery

# hostname = environ.get('RABBITMQ_HOST', 'localhost')
# model_key = environ.get('MODEL_KEY')
# run_key = environ.get('RUN_KEY')
# vhost = environ.get('RABBITMQ_VHOST', 'model-run')
# userid = environ.get('RABBITMQ_USERNAME', model_key)
# password = environ.get('RABBITMQ_PASSWORD', 'password')

app = Celery(
    'app',
    broker='pyamqp://guest@localhost//',
    include=['waterlp.tasks']
)

app.config_from_object('waterlp.celeryconfig')

if __name__ == '__main__':
    app.start()
