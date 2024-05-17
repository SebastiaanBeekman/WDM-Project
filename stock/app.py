import logging
import os
import atexit
import uuid
import requests

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response
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
    
    
class LogStockValue(Struct):
    key: str
    stockvalue: StockValue
    dateTime: str


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        abort(400, f"Item: {item_id} not found!")
    return entry



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
    

@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    stock_value = StockValue(stock=0, price=int(price))
    value = msgpack.encode(stock_value)
    id = get_id()
    log = msgpack.encode(LogStockValue(key=key, stockvalue=stock_value, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")))
    pipeline_db.set(id.text, log)
    pipeline_db.set(key, value)
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key, 'log': id.text})



@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    """This function apparenlty boeit niet."""
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(StockValue(stock=starting_stock, price=item_price))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )
    
@app.get('/log/<log_id>')
def find_log(log_id: str):
    log_entry: LogStockValue = get_log_from_db(log_id)
    return jsonify(
        {
            "key": log_entry.key,
            "stock": log_entry.stockvalue.stock,
            "price": log_entry.stockvalue.price,
            "dateTime": log_entry.dateTime
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    id = get_id()
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    log = msgpack.encode(LogStockValue(key=item_id, stockvalue=item_entry, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")))
    pipeline_db.set(item_id, msgpack.encode(item_entry))
    pipeline_db.set(id.text, log)
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}, log_id: {id.text}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    id = get_id()
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    log = msgpack.encode(LogStockValue(key=item_id, stockvalue=item_entry, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")))
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    pipeline_db.set(item_id, msgpack.encode(item_entry))
    pipeline_db.set(id.text, log)
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}, log_id: {id.text}", status=200)



@app.get('/log')
def get_stock_log():
    return jsonify(logging.getLoggerClass().root.handlers)
    


def get_id():
    try:
        response = requests.get(f"{GATEWAY_URL}/ids/create")
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
