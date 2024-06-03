import atexit
import logging
from flask import Flask
from datetime import datetime
import os
import redis

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ['GATEWAY_URL']

ID_COUNTER = "id-counter"

app = Flask("ids-service")

db: redis.Redis = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    password=os.environ['REDIS_PASSWORD'],
    db=int(os.environ['REDIS_DB'])
)
pipeline_db = db.pipeline()


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


@app.get('/create')
def create_id():
    counter = db.incr(ID_COUNTER)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    return f"log:{timestamp}{counter}"


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    db.set(ID_COUNTER, 0)  # start counter at zero on start-up
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    
    # app.logger.setLevel(logging.DEBUG)
