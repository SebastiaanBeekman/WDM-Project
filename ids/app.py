import logging
import time
from flask import Flask
from threading import Lock

sequence_lock = Lock()
counter = 0


DB_ERROR_STR = "DB error"

app = Flask("ids-service")


@app.get('/id/create/')
def create_id():
    global counter
    with sequence_lock:
        counter += 1
        return time.time_ns() + counter/1000        


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
