import os
import logging
import atexit
import uuid
import requests
from enum import Enum
import redis
from copy import deepcopy

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request
from datetime import datetime


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("stock-service")

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


class StockValue(Struct):
    stock: int
    price: int


class LogType(str, Enum):
    CREATE      = "Create"
    UPDATE      = "Update"
    DELETE      = "Delete"
    SENT        = "Sent"
    RECEIVED    = "Received"

class LogStatus(str, Enum):
    PENDING = "Pending"
    SUCCESS = "Success"
    FAILURE = "Failure"


class LogStockValue(Struct):
    id: str
    dateTime: str
    type: LogType | None = None
    status: LogStatus | None = None
    stock_id: str | None = None
    old_stockvalue: StockValue | None = None
    new_stockvalue: StockValue | None = None
    from_url: str | None = None
    to_url: str | None = None


def get_item_from_db(item_id: str, log_id: str | None = None) -> StockValue | None:
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None

    if entry is None:
        error_payload = LogStockValue(
            id=log_id if log_id else str(uuid.uuid4()),
            type=LogType.SENT,
            old_stockvalue=entry,
            stock_id=item_id,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
        db.set(get_key(), msgpack.encode(error_payload))
        return abort(400, f"Item: {item_id} not found!")
    return entry

### START OF LOG FUNCTIONS ###
def format_log_entry(log_entry: LogStockValue) -> dict:
    return {
        "id": log_entry.id,
        "type": log_entry.type,
        "status": log_entry.status,
        "stock_id": log_entry.stock_id,
        "stockValue:": {
            "old": {
                "stock": log_entry.old_stockvalue.stock if log_entry.old_stockvalue else None,
                "price": log_entry.old_stockvalue.price if log_entry.old_stockvalue else None
            },
            "new": {
                "stock": log_entry.new_stockvalue.stock if log_entry.new_stockvalue else None,
                "price": log_entry.new_stockvalue.price if log_entry.new_stockvalue else None
            }
        },
        "url": {
            "from": log_entry.from_url,
            "to": log_entry.to_url
        },
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
    return jsonify(formatted_log_entry), 200


@app.get('/logs')
def find_all_logs():
    try:
        # Retrieve all keys starting with "log:" from Redis
        log_keys = [key.decode('utf-8') for key in db.keys("log:*")]

        # Retrieve values corresponding to the keys
        logs = [{"p_key": key, "log": format_log_entry(msgpack.decode(db.get(key), type=LogStockValue))} for key in log_keys]

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
        logs = [{"id": key, "log": format_log_entry(msgpack.decode(db.get(key), type=LogStockValue))} for key in log_keys]

        return jsonify({'logs': logs}), 200
    except redis.exceptions.RedisError:
        return abort(500, 'Failed to retrieve logs from the database')
### END OF LOG FUNCTIONS ###

@app.post('/item/create/<price>')
def create_item(price: int):
    log_id = str(uuid.uuid4())

    # Create a log entry for the receieved request from the user
    received_payload_from_user = LogStockValue(
        id=log_id,
        type=LogType.RECEIVED,
        from_url=request.referrer,  # Endpoint that called this
        to_url=request.url,         # This endpoint
        status=LogStatus.PENDING,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(received_payload_from_user))

    item_id = str(uuid.uuid4())
    stock_value = StockValue(stock=0, price=int(price))

    # Create a log entry for the create request
    create_payload = LogStockValue(
        id=log_id,
        type=LogType.CREATE,
        stock_id=item_id,
        new_stockvalue=stock_value,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )

    # Set the log entry and the updated item in the pipeline
    log_key = get_key()
    pipeline_db.set(log_key, msgpack.encode(create_payload))
    pipeline_db.set(item_id, msgpack.encode(stock_value))
    try:
        pipeline_db.execute()
        app.logger.debug(f"Item: {item_id} created")
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        app.logger.debug(f"Item: {item_id} failed to create")
        return abort(400, DB_ERROR_STR)

    # Create a log entry for the sent response back to the user
    sent_payload_to_user = LogStockValue(
        id=log_id,
        type=LogType.SENT,
        from_url=request.url,       # This endpoint
        to_url=request.referrer,    # Endpoint that called this
        stock_id=item_id,
        status=LogStatus.SUCCESS,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(sent_payload_to_user))

    return jsonify({'item_id': item_id, 'log_key': log_key}), 200


@app.get('/find/<item_id>')
def find_item(item_id: str):
    log_id: str | None = request.args.get("log_id")
    log_id = log_id if log_id else str(uuid.uuid4())

    # Create a log entry for the receieved request from the user
    received_payload_from_user = LogStockValue(
        id=log_id,
        type=LogType.RECEIVED,
        from_url=request.referrer,  # Endpoint that called this
        to_url=request.url,         # This endpoint
        stock_id=item_id,
        status=LogStatus.PENDING,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(received_payload_from_user))

    # Retrieve the item from the database
    item_entry: StockValue = get_item_from_db(item_id, log_id)

    # Create a log entry for the sent response
    sent_payload_to_user = LogStockValue(
        id=log_id,
        type=LogType.SENT,
        from_url=request.url,       # This endpoint
        to_url=request.referrer,    # Endpoint that called this
        stock_id=item_id,
        status=LogStatus.SUCCESS,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(sent_payload_to_user))

    # Return the item
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    log_id: str | None = request.args.get("log_id")
    log_id = log_id if log_id else str(uuid.uuid4())
    
    # Create a log entry for the receieved request from the user
    received_payload_from_user = LogStockValue(
        id=log_id,
        type=LogType.RECEIVED,
        from_url=request.referrer,  # Endpoint that called this
        to_url=request.url,         # This endpoint
        stock_id=item_id,
        status=LogStatus.PENDING,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(received_payload_from_user))

    item_entry: StockValue = get_item_from_db(item_id)
    old_item_entry: StockValue = deepcopy(item_entry)
    
    item_entry.stock += int(amount)
    
    # Create a log entry for the update request
    update_payload = LogStockValue(
        id=log_id,
        type=LogType.UPDATE,
        stock_id=item_id,
        old_stockvalue=old_item_entry,
        new_stockvalue=item_entry,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )

    # Set the log entry and the updated item in the pipeline
    log_key = get_key()
    pipeline_db.set(log_key, msgpack.encode(update_payload))
    pipeline_db.set(item_id, msgpack.encode(item_entry))
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        app.logger.debug(f"Item: {item_id} failed to update")
        return abort(400, DB_ERROR_STR)
    
    # Create a log entry for the sent response back to the user
    sent_payload_to_user = LogStockValue(
        id=log_id,
        type=LogType.SENT,
        from_url=request.url,       # This endpoint
        to_url=request.referrer,    # Endpoint that called this
        stock_id=item_id,
        status=LogStatus.SUCCESS,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(sent_payload_to_user))
    
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}, log_key: {log_key}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    log_id: str | None = request.args.get("log_id")
    log_id = log_id if log_id else str(uuid.uuid4())
    
    # Create a log entry for the receieved request from the user
    received_payload_from_user = LogStockValue(
        id=log_id,
        type=LogType.RECEIVED,
        from_url=request.referrer,  # Endpoint that called this
        to_url=request.url,         # This endpoint
        stock_id=item_id,
        status=LogStatus.PENDING,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(received_payload_from_user))
    
    item_entry: StockValue = get_item_from_db(item_id)
    old_item_entry: StockValue = deepcopy(item_entry)
    
    item_entry.stock -= int(amount)
    
    if item_entry.stock < 0:
        error_payload = LogStockValue(
            id=log_id,
            type=LogType.SENT,
            stock_id=item_id,
            old_stockvalue=old_item_entry,
            new_stockvalue=item_entry,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
        db.set(get_key(), msgpack.encode(error_payload))
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
        
    # Create a log entry for the update request
    update_payload = LogStockValue(
        id=log_id,
        type=LogType.UPDATE,
        stock_id=item_id,
        old_stockvalue=old_item_entry,
        new_stockvalue=item_entry,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    
    log_key = get_key()
    pipeline_db.set(log_key, msgpack.encode(update_payload))
    pipeline_db.set(item_id, msgpack.encode(item_entry))
    try:
        pipeline_db.execute()
        app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        app.logger.debug(f"Item: {item_id} failed to update")
        return abort(400, DB_ERROR_STR)
    
    # Create a log entry for the sent response back to the user
    sent_payload_to_user = LogStockValue(
        id=log_id,
        type=LogType.SENT,
        from_url=request.url,       # This endpoint
        to_url=request.referrer,    # Endpoint that called this
        stock_id=item_id,
        status=LogStatus.SUCCESS,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(sent_payload_to_user))
    
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}, log_key: {log_key}", status=200)


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


def get_key():
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
    # app.logger.setLevel(logging.DEBUG)
