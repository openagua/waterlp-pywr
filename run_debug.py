from waterlp.celery_app import app
app.start(['celery', 'worker', '-l', 'INFO'])