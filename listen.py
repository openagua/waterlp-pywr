#!/usr/bin/env python
import json
import getpass
from shutil import rmtree
import pika
import os
from main import commandline_parser, run_model

from waterlp.logger import RunLogger


def callback(ch, method, properties, body):
    message = json.loads(body)
    env = message.get('env', {})
    args = message.get('args')
    kwargs = message.get('kwargs')

    app_dir = '/home/{}/.waterlp'.format(getpass.getuser())
    logs_dir = '{}/logs'.format(app_dir)

    # if os.path.exists(app_dir):
    #     rmtree(app_dir)
    # os.makedirs(logs_dir)

    for key, value in env.items():
        os.environ[key] = value
    print(" [x] Running model with %r" % args)

    parser = commandline_parser()
    args, unknown = parser.parse_known_args(args)

    RunLog = RunLogger(name='waterlp', app_name=args.app_name, run_name=args.run_name, logs_dir=logs_dir,
                       username=args.hydra_username)

    try:
        RunLog.log_start()
        run_model(args, logs_dir, **kwargs)
        RunLog.log_finish()
    except Exception as err:
        RunLog.log_error(message=str(err))


def start_listening(model_key):

    # set up logging
    app_dir = '/home/{}/.waterlp'.format(getpass.getuser())
    logs_dir = '{}/logs'.format(app_dir)
    if os.path.exists(app_dir):
        rmtree(app_dir)
    os.makedirs(logs_dir)

    queue_name = 'model-{}'.format(model_key)
    run_key = os.environ.get('RUN_KEY')
    if run_key:
        queue_name += '-{}'.format(run_key)
    host = os.environ.get('RABBITMQ_HOST', 'localhost')
    if host == 'localhost':
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    else:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=host,
            virtual_host=os.environ.get('RABBITMQ_VHOST', 'model-run'),
            credentials=pika.PlainCredentials(
                username=os.environ.get('RABBITMQ_USERNAME', model_key),
                password=os.environ.get('RABBITMQ_PASSWORD', 'password')
            )
        ))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)
    channel.basic_consume(callback, queue=queue_name, no_ack=True)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        model_key = os.environ['MODEL_KEY']
        start_listening(model_key)
    except:
        raise
