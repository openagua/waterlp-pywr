#!/usr/bin/env python
import json
import pika
import os
from main import run_model


def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        env = message.get('env', {})
        args = message.get('args')
        kwargs = message.get('kwargs')

        for key, value in env.items():
            os.environ[key] = value
        print(" [x] Running model with %r" % args)
        run_model(args_list=args, **kwargs)
    except:
        pass  # fail silently for now


def start_listening(model_key):
    queue_name = 'model-{}'.format(model_key)
    run_secret = os.environ.get('RUN_KEY')
    if run_secret:
        queue_name += '-{}'.format(run_secret)
    host = os.environ.get('RABBITMQ_HOST', 'localhost')
    if host == 'localhost':
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
    else:
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=host,
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
