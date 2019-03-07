#!/usr/bin/env python
import getpass
from shutil import rmtree
from kombu import Connection, Queue
from kombu.mixins import ConsumerMixin
import os
from os import environ

from waterlp.main import commandline_parser, run_model
from waterlp.logger import RunLogger
from waterlp.reporters.redis import local_redis
from waterlp.utils.application import message_handler, PNSubscribeCallback

from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub


# This code is derived from
# https://medium.com/python-pandemonium/building-robust-rabbitmq-consumers-with-python-and-kombu-part-2-e9505f56e12e

class Worker(ConsumerMixin):

    def __init__(self, connection, queues):
        self.connection = connection
        self.queues = queues

    def get_consumers(self, Consumer, channel):
        run_consumer = Consumer(queues=self.queues,
                                prefetch_count=0,
                                # no_ack=True,
                                callbacks=[self.process_task])
        return [run_consumer]

    def process_task(self, body, message):

        message.ack() # acknowledge right away to prevent repeating a failed task

        # 2) start modeling based on instructions
        action = body.get('action')
        if action is not None and action != 'start':
            return

        env = body.get('env', {})
        args = body.get('args')
        kwargs = body.get('kwargs')

        app_dir = '/home/{}/.waterlp'.format(getpass.getuser())
        logs_dir = '{}/logs'.format(app_dir)

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


if __name__ == '__main__':

    hostname = os.environ.get('RABBITMQ_HOST', 'localhost')
    model_key = os.environ.get('MODEL_KEY')
    run_key = os.environ.get('RUN_KEY')
    vhost = os.environ.get('RABBITMQ_VHOST', 'model-run')
    userid = os.environ.get('RABBITMQ_USERNAME', model_key)
    password = os.environ.get('RABBITMQ_PASSWORD', 'password')

    app_dir = '/home/{}/.waterlp'.format(getpass.getuser())
    logs_dir = '{}/logs'.format(app_dir)
    if os.path.exists(app_dir):
        rmtree(app_dir)
    os.makedirs(logs_dir)

    # Note: heartbeat is needed to ensure dead connections are terminated right away.
    # heartbeat only works with py-amqp (so don't install librabbitmq).
    # see https://kombu.readthedocs.io/en/stable/reference/kombu.connection.html#kombu.connection.Connection.heartbeat
    # ...and https://www.rabbitmq.com/heartbeats.html
    # heartbeat = 10 results in an actual ping (and light network traffic, good for keeping a TCP connection alive) every 5 seconds
    # Hopefully this will not cause too major a performance hit.

    url = 'amqp://{username}:{password}@{hostname}/{vhost}'.format(
        username=userid,
        password=password,
        hostname=hostname,
        vhost=vhost,
    )

    queue_name = 'model-{}'.format(model_key)
    if run_key:
        queue_name += '-{}'.format(run_key)

    pnconfig = PNConfiguration()
    pnconfig.subscribe_key = environ.get('PUBNUB_SUBSCRIBE_KEY')
    pnconfig.ssl = False
    pubnub = PubNub(pnconfig)
    pubnub.add_listener(PNSubscribeCallback())

    pubnub.subscribe().channels(queue_name).execute()
    print(" [*] Subscribed to PubNub")

    with Connection(url, heartbeat=10) as conn:
        try:

            # QUEUE
            task_queue = Queue(
                name=queue_name,
                durable=True,
                auto_delete=False,
                message_ttl=3600,
            )

            worker = Worker(conn, [task_queue])

            print(' [*] Waiting for messages. To exit press CTRL+C')

            worker.run()
        except KeyboardInterrupt:
            pubnub.unsubscribe_all()
            conn.release()
            print('bye bye')
