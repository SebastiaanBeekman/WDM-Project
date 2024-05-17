import logging
from flask import Flask
from threading import Lock
from datetime import datetime

sequence_lock = Lock()

app = Flask("ids-service")


counter = 0

@app.get('/create')
def create_id():
    global counter
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    with sequence_lock:
        counter += 1
        return f"log:{timestamp}{counter}"


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
