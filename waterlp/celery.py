from celery import Celery

app = Celery(
    'app',
    broker='pyamqp://guest@localhost//',
    include=['waterlp.tasks']
)

app.config_from_object('waterlp.celeryconfig')

if __name__ == '__main__':

    # TODO: migrate listen/Kombu to here (100% Celery)
    # see: https://www.distributedpython.com/2018/06/19/call-celery-task-outside-codebase/

    try:
        app.start()

    except KeyboardInterrupt:
        # thread.release()
        print('Stopped by user.')
