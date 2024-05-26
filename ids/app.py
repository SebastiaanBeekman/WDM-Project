import logging
from flask import Flask, abort
from datetime import datetime
import os
from pymemcache.client import base



app = Flask("ids-service")

client = base.Client(("memcached", 11211))
client.set("counter", 1)

@app.get('/create')
def create_id():
    counter = int(client.get("counter").decode('utf-8'))
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    counter += 1
    client.set("counter", counter)
    return f"log:{timestamp}{counter}"
  


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
