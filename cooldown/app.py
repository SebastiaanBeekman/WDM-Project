import logging
from flask import Flask, abort
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import os
import atexit
import time
from datetime import datetime
from pymemcache.client import base


app = Flask("cooldown-service")
GATEWAY_URL = os.environ['GATEWAY_URL']
REQ_ERROR_STR = "Requests error"


client = base.Client(("memcached", 11211))
client.set("last_cooldown", None)

cooldown_duration = 5
    
def start_cooldown():
    general_url = f'/cooldown_start/{client.get("last_cooldown").decode('utf-8')}'
    stock_url = f'{GATEWAY_URL}/stock{general_url}'
    order_url = f'{GATEWAY_URL}/orders{general_url}'
    payment_url = f'{GATEWAY_URL}/payment{general_url}'
    try:
        requests.post(stock_url)
        requests.post(order_url)
        requests.post(payment_url)
        time.sleep(cooldown_duration)
        client.set("last_cooldown", datetime.now().strftime("%Y%m%d%H%M%S%f"))
        stop_cooldown()
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
     
        
def stop_cooldown():
    general_url = f'/cooldown_stop'
    order_url = f'{GATEWAY_URL}/orders{general_url}'
    try:
        requests.post(order_url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    
    

scheduler = BackgroundScheduler()
scheduler.add_job(start_cooldown, 'interval', seconds=10+cooldown_duration)
scheduler.start()

atexit.register(lambda: scheduler.shutdown())


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
