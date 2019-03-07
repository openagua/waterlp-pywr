from celery import Celery

app = Celery(
    'app',
    broker='pyamqp://guest@localhost//',
    include=['waterlp.tasks']
)

app.config_from_object('waterlp.celeryconfig')

if __name__ == '__main__':

    # model_key = environ.get('MODEL_KEY')
    # run_key = environ.get('RUN_KEY')
    #
    # queue_name = 'model-{}'.format(model_key)
    # if run_key:
    #     queue_name += '-{}'.format(run_key)
    # p = global_redis.pubsub()
    # p.subscribe(**{queue_name: message_handler})
    # thread = p.run_in_thread(sleep_time=0.001)
    # print("Subscribed to: " + queue_name)

    try:
        app.start()

    except KeyboardInterrupt:
        # thread.release()
        print('Stopped by user.')
