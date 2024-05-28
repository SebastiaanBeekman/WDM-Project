import os
import logging
import atexit
import uuid
import requests
from enum import Enum
import redis
from copy import deepcopy
from collections import defaultdict
from ast import literal_eval

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request
# from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta


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
# atexit.register(lambda: scheduler.shutdown())

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
        log_key = get_key()
        error_payload = LogStockValue(
            id=log_id if log_id else str(uuid.uuid4()),
            type=LogType.SENT,
            stock_id=item_id,
            from_url=request.url,
            to_url=request.referrer,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
        db.set(log_key, msgpack.encode(error_payload))
        return abort(400, f"Item: {item_id} not found! Log key: {log_key}")
    return entry

########################################################################################################################
#   START OF LOG FUNCTIONS
########################################################################################################################
def format_log_entry(log_entry: LogStockValue) -> dict:
    return {
        "id": log_entry.id,
        "type": log_entry.type,
        "status": log_entry.status,
        "stock_id": log_entry.stock_id,
        "stock_value": {
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
        "date_time": log_entry.dateTime
    }


def sort_logs(logs: list[dict]):
    log_dict = defaultdict(list)
    for log in logs:
        log_dict[log["log"]["id"]].append(log)
    
    for key in log_dict:
        log_dict[key] = sorted(log_dict[key], key=lambda x: x["log"]["date_time"])
    
    return log_dict


def get_log_from_db(log_id: str) -> LogStockValue | None:
    try:
        entry: bytes = db.get(log_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    entry: LogStockValue | None = msgpack.decode(entry, type=LogStockValue) if entry else None
    
    if entry is None:
        abort(400, f"Log: {log_id} not found!")
    return entry

@app.get('/log_count')
def get_log_count():
    return Response(str(len(db.keys("log:*"))), status=200)


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
    try:
        # Retrieve all keys starting with "log:[number]" from Redis
        log_keys = [key.decode('utf-8') for key in db.keys(f"log:{number}*")]

        # Retrieve values corresponding to the keys
        logs = [{"id": key, "log": format_log_entry(msgpack.decode(db.get(key), type=LogStockValue))} for key in log_keys]

        return jsonify({'logs': logs}), 200
    except redis.exceptions.RedisError:
        return abort(500, 'Failed to retrieve logs from the database')
    
def find_all_logs_time(time: datetime, min_diff: int = 5):
    try:
        # Calculate the range
        lower_bound: datetime = time - timedelta(minutes=min_diff)
        upper_bound: datetime = time

        # Create a broad pattern to fetch all potential keys
        broad_pattern = "log:*"
        
        # Retrieve all keys matching the broad pattern
        potential_keys = [key.decode('utf-8') for key in db.keys(broad_pattern)]

        # Filter keys based on the regex pattern
        logs = []
        for key in potential_keys:
            timestamp_str = key.split(":")[-1][:20]
            key_timestamp: datetime = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S%f")
            
            if lower_bound > key_timestamp or upper_bound < key_timestamp:
                continue
            
            raw_data = db.get(key)
            
            if not raw_data:
                continue
                
            log_entry = msgpack.decode(raw_data, type=LogStockValue)
            logs.append({"id": key, "log": format_log_entry(log_entry)})

        return logs
    except redis.exceptions.RedisError:
        return abort(500, 'Failed to retrieve logs from the database')


@app.get('/sorted_logs/<min_diff>')
def find_sorted_logs(min_diff: int):
    time: datetime = datetime.now()
    logs = find_all_logs_time(time, int(min_diff))
    sorted_logs = sort_logs(logs)
    
    return jsonify(sorted_logs), 200


@app.post('/log/create')
def create_log():
    str_dict_log_entry = str(request.get_json())
    dict_log_entry = literal_eval(str_dict_log_entry)
    log_entry = LogStockValue(**dict_log_entry)
    
    log_key = get_key()
    db.set(log_key, msgpack.encode(log_entry))
    
    return jsonify({"msg": "Log entry created", "log_key": log_key}), 200

########################################################################################################################
#   START OF BENCHMARK FUNCTIONS
########################################################################################################################
@app.post('/item/create/<price>/benchmark')
def create_item_benchmark(price: int):
    item_id = str(uuid.uuid4())
    stock_value = StockValue(stock=0, price=int(price))
    
    try:
        db.set(item_id, msgpack.encode(stock_value))
    except redis.exceptions.RedisError:        
        return abort(400, DB_ERROR_STR)

    return jsonify({'item_id': item_id}), 200


@app.get('/find/<item_id>/benchmark')
def find_item_benchmark(item_id: str):
    entry: bytes = db.get(item_id)
    item_entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None

    if item_entry is None:
        return abort(400, f"Item: {item_id} not found!")

    return jsonify({"stock": item_entry.stock, "price": item_entry.price}), 200

@app.post('/add/<item_id>/<amount>/benchmark')
def add_stock_benchmark(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock += int(amount)
    
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:        
        return abort(400, DB_ERROR_STR)

    return jsonify({"stock": item_entry.stock}), 200

@app.post('/subtract/<item_id>/<amount>/benchmark')
def remove_stock_benchmark(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    item_entry.stock -= int(amount)
    
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:        
        return abort(400, DB_ERROR_STR)

    return jsonify({"stock": item_entry.stock}), 200

########################################################################################################################
#   START OF MICROSERVICE FUNCTIONS
########################################################################################################################
# Log Order: 
# Success: RECEIVED -> CREATE -> SENT (success)
# Failure: RECEIVED -> SENT (error)
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
    app.logger.debug(f"Item: {item_id} created") # Keep this for benchmarking purposes

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
    except redis.exceptions.RedisError:
        error_payload = LogStockValue(
            id=log_id,
            type=LogType.SENT,
            from_url=request.url,       # This endpoint
            to_url=request.referrer,    # Endpoint that called this
            stock_id=item_id,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
        db.set(get_key(), msgpack.encode(error_payload))
        
        pipeline_db.discard()
        
        return abort(400, DB_ERROR_STR)
    
    # Fault Tollerance: CRASH - Undo

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

    return jsonify({'item_id': item_id, 'log_id': log_id}), 200


# Log Order: RECEIVED -> SENT
# Fault Tollerance: do nothing
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
    return jsonify({"stock": item_entry.stock, "price": item_entry.price, "log_id": log_id}), 200


# Log Order: 
# Success: RECEIVED -> UPDATE -> SENT (success)
# Failure: RECEIVED -> SENT (error)
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
    
    # Update the stock value
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
        error_payload = LogStockValue(
            id=log_id,
            type=LogType.SENT,
            from_url=request.url,       # This endpoint
            to_url=request.referrer,    # Endpoint that called this
            stock_id=item_id,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
        db.set(get_key(), msgpack.encode(error_payload))
        
        pipeline_db.discard()
        
        return abort(400, DB_ERROR_STR)
    
    # Fault Tollerance: CRASH - Undo

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

    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}, log_id: {log_id}", status=200)


# Log Order: 
# Success: RECEIVED -> UPDATE -> SENT (success)
# Failure: RECEIVED -> SENT (error)
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
    
    # Update stock
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}") # Keep this for benchmarking purposes
    
    if item_entry.stock < 0:
        # Create a log entry for the error
        error_payload = LogStockValue(
            id=log_id,
            type=LogType.SENT,
            stock_id=item_id,
            old_stockvalue=old_item_entry,
            new_stockvalue=item_entry,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
        log_key = get_key()
        db.set(log_key, msgpack.encode(error_payload))
        abort(400, f"Item: {item_id} stock cannot get reduced below zero! Log key: {log_key}")
        
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
        error_payload = LogStockValue(
            id=log_id,
            type=LogType.SENT,
            from_url=request.url,       # This endpoint
            to_url=request.referrer,    # Endpoint that called this
            stock_id=item_id,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
        db.set(get_key(), msgpack.encode(error_payload))
        
        pipeline_db.discard()
        
        return abort(400, DB_ERROR_STR)
    
    # Fault Tollerance: CRASH - Undo
    
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
    
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}, log_id: {log_id}", status=200)


def get_key():
    try:
        response = requests.get(f"{GATEWAY_URL}/ids/create")
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response.text


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
    

def fix_consistency():
    time: datetime = datetime.now()
    logs = find_all_logs_time(time, 1)
    # app.logger.debug(logs)
    # app.logger.debug(time)
    
    log_dict = defaultdict(list)
    for log in logs:
        log_dict[log["log"]["id"]].append(log)
    
    for key in log_dict:
        log_dict[key] = sorted(log_dict[key], key=lambda x: x["log"]["date_time"])

@app.get('/fault_tollerance/<min_diff>')
def test_fault_tollerance(min_diff: int):
    fix_fault_tollerance(int(min_diff))
    return jsonify({"msg": "Fault Tollerance Successful"}), 200


def fix_fault_tollerance(min_diff: int = 5):
    time: datetime = datetime.now()
    logs = find_all_logs_time(time, int(min_diff))
    sorted_logs = sort_logs(logs)
    
    for _, log_list in sorted_logs.items():
        last_log = log_list[-1]["log"]
        if last_log["status"] in [LogStatus.SUCCESS, LogStatus.FAILURE] and last_log["type"] == LogType.SENT: # If log was finished properly
            continue
        
        for log_entry in reversed(log_list):
            log = log_entry["log"]
            
            log_type = log["type"]
            log_stock_id = log["stock_id"]
            if log_type == LogType.CREATE:
                db.delete(log_stock_id)
            elif log_type == LogType.UPDATE:
                log_stock_old = log["stock_value"]["old"]
                db.set(log_stock_id, msgpack.encode(StockValue(stock=log_stock_old["stock"], price=log_stock_old["price"])))
            
            db.delete(log_entry["id"])
    
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    # app.logger.setLevel(gunicorn_logger.level)
    app.logger.setLevel(logging.DEBUG)
    
    # fix_fault_tollerance()
    