import logging
import os
import atexit
import random
import uuid
from collections import defaultdict
from datetime import datetime
from enum import Enum

import redis
import requests

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))
pipeline_db = db.pipeline()

def close_db_connection():
    db.close()

atexit.register(close_db_connection)

class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int
    
class LogType(Enum):
    CREATE = 1
    UPDATE = 2
    DELETE = 3
    SENT = 4
    RECEIVED = 5

    
class LogStatus(Enum):
    PENDING = 1
    SUCCESS = 2
    FAILURE = 3

class LogOrderValue(Struct):
    key: str
    type: LogType | None
    status: LogStatus | None
    ordervalue: OrderValue | None
    url: str | None
    dateTime: str


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str, optional_param: dict[str, str] = {}):
    url = url_for(url, **optional_param) # The ** operator unpacks the dictionary into keyword arguments
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


def get_log_from_db(log_id: str) -> LogOrderValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(log_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: LogOrderValue | None = msgpack.decode(entry, type=LogOrderValue) if entry else None
    if entry is None:
        abort(400, f"Log: {log_id} not found!")
    return entry


@app.get('/logs')
def get_all_logs():
    try:
        # Retrieve all keys starting with "log:" from Redis
        log_keys = [key.decode('utf-8') for key in db.keys("log:*")]
        
        # Retrieve values corresponding to the keys
        logs = [{"id": key, "log": msgpack.decode(db.get(key))} for key in log_keys]

        return jsonify({'logs': logs}), 200
    except redis.exceptions.RedisError:
        return abort(500, 'Failed to retrieve logs from the database')
    

@app.get('/logs_from/<number>')
def get_all_logs_from(number: int):
    """This function is still broken."""
    try:
        # Retrieve all keys starting with "log:" from Redis
        log_keys = [key.decode('utf-8') for key in db.keys(f"log:{number}*")]
        
        # Retrieve values corresponding to the keys
        logs = [{"id": key, "log": msgpack.decode(db.get(key))} for key in log_keys]

        return jsonify({'logs': logs}), 200
    except redis.exceptions.RedisError:
        return abort(500, 'Failed to retrieve logs from the database')



@app.post('/create/<user_id>')
def create_order(user_id: str):
    id = get_id()
    key = str(uuid.uuid4())
    ordervalue = OrderValue(paid=False, items=[], user_id=user_id, total_cost=0)
    value = msgpack.encode(ordervalue)
    log = msgpack.encode(LogOrderValue(key=key, ordervalue=ordervalue, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"), type=logType.CREATE))
    pipeline_db.set(key, value)
    pipeline_db.set(id.text, log)
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key, 'log_id': id.text})


# Can Ignore
@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(paid=False,
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    log_id = str(uuid.uuid4())
    url = f"{GATEWAY_URL}/order/addItem/{order_id}/{item_id}/{quantity}"
    
    # Create a log entry for the received request
    received_payload = LogOrderValue(
        key=log_id, 
        type=LogType.RECEIVED,
        url=url,
        status=LogStatus.PENDING,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
    )
    db.set(get_id(), msgpack.encode(received_payload))
    
    request_url = f"{GATEWAY_URL}/stock/find/{item_id}"
    
    # Create a log entry for the received request
    sent_payload = LogOrderValue(
        key=log_id, 
        type=LogType.SENT,
        url=request_url,
        status=LogStatus.PENDING,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"), 
    )
    db.set(get_id(), msgpack.encode(sent_payload))
    
    # Send the request
    item_reply = send_get_request(request_url, {"log_id": log_id})
    
    # Create a log entry for the received response (success or failure)
    received_payload = LogOrderValue(
        key=log_id,
        type=LogType.RECEIVED,
        url=request_url,
        status=LogStatus.SUCCESS if item_reply.status_code == 200 else LogStatus.FAILURE,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
    )
    db.set(get_id(), msgpack.encode(received_payload))
    
    # Request failed because item does not exist
    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")
    
    # Locally update the order value
    item_json: dict = item_reply.json()
    order_entry: OrderValue = get_order_from_db(order_id)
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    
    # Create a log entry for the update request
    update_payload = LogOrderValue(
        key=log_id, 
        type=LogType.UPDATE,
        url=url,
        ordervalue=order_entry, 
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"), 
    )
    
    # Set the log entry and the updated order value in the pipeline
    pipeline_db.set(get_id(), msgpack.encode(update_payload))
    pipeline_db.set(order_id, msgpack.encode(order_entry))
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    
    # Create a log for the sent response
    sent_payload = LogOrderValue(
        key=log_id,
        type=LogType.SENT,
        url=url,
        status=LogStatus.SUCCESS,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
    )
    db.set(get_id(), msgpack.encode(sent_payload))
    
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}, log_id: {log_id.text}", status=200)


def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    id = get_id()
    app.logger.debug(f"Checking out {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id)
    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    # The removed items will contain the items that we already have successfully subtracted stock from
    # for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        stock_reply = send_post_request(f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}")
        if stock_reply.status_code != 200:
            # If one item does not have enough stock we need to rollback
            rollback_stock(removed_items)
            abort(400, f'Out of stock on item_id: {item_id}')
        removed_items.append((item_id, quantity))
    user_reply = send_post_request(f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}")
    if user_reply.status_code != 200:
        # If the user does not have enough credit we need to rollback all the item stock subtractions
        rollback_stock(removed_items)
        abort(400, "User out of credit")
    order_entry.paid = True
    log = msgpack.encode(LogOrderValue(key=order_id, ordervalue=order_entry, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")))
    pipeline_db.set(order_id, msgpack.encode(order_entry))
    pipeline_db.set(id.text, log)
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    app.logger.debug("Checkout successful")
    return Response(f"Checkout successful, log: {id.text}", status=200)


@app.get('/log/<log_id>')
def find_log(log_id: str):
    log_entry: LogOrderValue = get_log_from_db(log_id)
    return jsonify(
        {
            "key": log_entry.key,
            "paid": log_entry.ordervalue.paid,
            "items": log_entry.ordervalue.items,
            "user_id": log_entry.ordervalue.user_id,
            "total_cost": log_entry.ordervalue.total_cost,
            "dateTime": log_entry.dateTime
        }
    )


def get_id():
    try:
        response = requests.get(f"{GATEWAY_URL}/ids/create")
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response.text


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
