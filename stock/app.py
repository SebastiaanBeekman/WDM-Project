import logging
import os
import atexit
import uuid
import requests
from enum import Enum
import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request
from datetime import datetime


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("stock-service")


db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))
pipeline_db = db.pipeline()


def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


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


class LogStockValue(Struct):
    key: str
    dateTime: str
    type: LogType | None = None
    status: LogStatus | None = None
    stockvalue: StockValue | None = None
    url: str | None = None


def get_item_from_db(item_id: str, log_id: str | None = None, url: str | None = None) -> StockValue | None:
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None

    if entry is None:
        log_payload = LogStockValue(
            key=log_id if log_id else str(uuid.uuid4()),
            type=LogType.SENT,
            url=url,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
        db.set(get_id(), msgpack.encode(log_payload))
        return abort(400, f"Item: {item_id} not found!")
    return entry

### START OF LOG FUNCTIONS ###
def format_log_entry(log_entry: LogStockValue) -> dict:
    return {
        "key": log_entry.key,
        "type": log_entry.type,
        "status": log_entry.status,
        "stockvalue": {
            "stock": log_entry.stockvalue.stock if log_entry.stockvalue else None,
            "price": log_entry.stockvalue.price if log_entry.stockvalue else None
        },
        "url": log_entry.url,
        "dateTime": log_entry.dateTime
    }

def get_log_from_db(log_id: str) -> LogStockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(log_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: LogStockValue | None = msgpack.decode(entry, type=LogStockValue) if entry else None
    if entry is None:
        abort(400, f"Log: {log_id} not found!")
    return entry


@app.get('/log/<log_id>')
def find_log(log_id: str):
    log_entry: LogStockValue = get_log_from_db(log_id)
    formatted_log_entry : dict = format_log_entry(log_entry)
    return jsonify(
        {
        "key": log_entry.key,
        "type": log_entry.type,
        "status": log_entry.status,
        "stockvalue": {
            "stock": log_entry.stockvalue.stock if log_entry.stockvalue else None,
            "price": log_entry.stockvalue.price if log_entry.stockvalue else None
        },
        "url": log_entry.url,
        "dateTime": log_entry.dateTime
    }
    )

def format_log_entry(log_entry: LogStockValue):
    return {
        "key": log_entry.key,
        "type": log_entry.type,
        "status": log_entry.status,
        "stockvalue": {
            "stock": log_entry.stockvalue.stock if log_entry.stockvalue else None,
            "price": log_entry.stockvalue.price if log_entry.stockvalue else None
        },
        "url": log_entry.url,
        "dateTime": log_entry.dateTime
    }


@app.get('/logs')
def find_all_logs():
    try:
        # Retrieve all keys starting with "log:" from Redis
        log_keys = [key.decode('utf-8') for key in db.keys("log:*")]

        # Retrieve values corresponding to the keys
        logs = [{"p_key": key, "log": find_log(key)} for key in log_keys]

        return jsonify({'logs': logs}), 200
    except redis.exceptions.RedisError:
        return abort(500, 'Failed to retrieve logs from the database')


@app.get('/logs_from/<number>')
def find_all_logs_from(number: int):
    """This function is still broken."""

    # log_id = request.args.get('log_id')

    try:
        # Retrieve all keys starting with "log:[number]" from Redis
        log_keys = [key.decode('utf-8') for key in db.keys(f"log:{number}*")]

        # Retrieve values corresponding to the keys
        logs = [{"id": key, "log": msgpack.decode(db.get(key))} for key in log_keys]

        return jsonify({'logs': logs}), 200
    except redis.exceptions.RedisError:
        return abort(500, 'Failed to retrieve logs from the database')

### END OF LOG FUNCTIONS ###

@app.post('/item/create/<price>')
def create_item(price: int):
    log_id = str(uuid.uuid4())
    url = f"/item/create/{price}"

    # Create a log entry for the receieved request
    received_payload_from_user = LogStockValue(
        key=log_id,
        type=LogType.RECEIVED,
        url=url,
        status=LogStatus.PENDING,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_id(), msgpack.encode(received_payload_from_user))

    item_id = str(uuid.uuid4())
    stock_value = StockValue(stock=0, price=int(price))

    # Create a log entry for the create request
    create_payload = LogStockValue(
        key=log_id,
        type=LogType.CREATE,
        url=url,
        stockvalue=stock_value,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )

    # Set the log entry and the updated item in the pipeline
    log_key = get_id()
    pipeline_db.set(log_key, msgpack.encode(create_payload))
    pipeline_db.set(item_id, msgpack.encode(stock_value))
    try:
        pipeline_db.execute()
        app.logger.debug(f"Item: {item_id} created")
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        app.logger.debug(f"Item: {item_id} failed to create")
        return abort(400, DB_ERROR_STR)

    sent_payload_to_user = LogStockValue(
        key=log_id,
        type=LogType.SENT,
        url=url,
        status=LogStatus.SUCCESS,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_id(), msgpack.encode(sent_payload_to_user))

    return jsonify({'item_id': item_id, 'log': log_key})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    log_id: str | None = request.args.get('log_id')  # Optional query parameter (Passed in app, but not in Postman)
    url = f"/find/{item_id}"

    # Create a log entry for the receieved request
    received_payload = LogStockValue(
        key=log_id if log_id else str(uuid.uuid4()),
        type=LogType.RECEIVED,
        url=url,
        status=LogStatus.PENDING,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_id(), msgpack.encode(received_payload))

    # Retrieve the item from the database
    item_entry: StockValue = get_item_from_db(item_id, log_id, url)

    # Create a log entry for the sent response
    sent_payload = LogStockValue(
        key=log_id if log_id else str(uuid.uuid4()),
        type=LogType.SENT,
        url=url,
        status=LogStatus.SUCCESS,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_id(), msgpack.encode(sent_payload))

    # Return the item
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    s_id = get_id()
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    log = msgpack.encode(
        LogStockValue(
            key=item_id,
            stockvalue=item_entry,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
    )

    pipeline_db.set(item_id, msgpack.encode(item_entry))
    pipeline_db.set(s_id, log)
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}, log_id: {s_id}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    id = get_id()
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    log = msgpack.encode(
        LogStockValue(
            key=item_id,
            stockvalue=item_entry,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
    )
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    pipeline_db.set(item_id, msgpack.encode(item_entry))
    pipeline_db.set(id, log)
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}, log_id: {id}", status=200)


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    """This function apparenlty boeit niet."""
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {
        f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
        for i in range(n)
    }
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


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
