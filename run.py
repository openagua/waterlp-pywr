from waterlp import app
app.start(['celery', 'worker', '-l', 'WARNING'])