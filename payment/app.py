import logging
import os
import atexit
import uuid
import requests
from datetime import datetime

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ['GATEWAY_URL']


app = Flask("payment-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))

pipeline_db = db.pipeline()

def close_db_connection():
    db.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int
    
    
class LogUserValue(Struct):
    key: str
    uservalue: UserValue
    dateTime: str


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


def get_log_from_db(log_id: str) -> LogUserValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(log_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: LogUserValue | None = msgpack.decode(entry, type=LogUserValue) if entry else None
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
    try:
        # Retrieve all keys starting with "log:" from Redis
        log_keys = [key.decode('utf-8') for key in db.keys(f"log:{number}*")]
        
        # Retrieve values corresponding to the keys
        logs = [{"id": key, "log": msgpack.decode(db.get(key))} for key in log_keys]

        return jsonify({'logs': logs}), 200
    except redis.exceptions.RedisError:
        return abort(500, 'Failed to retrieve logs from the database')


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    uservalue = UserValue(credit=0)
    value = msgpack.encode(uservalue)
    id = get_id()
    log = msgpack.encode(LogUserValue(key=key, uservalue=uservalue, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")))
    pipeline_db.set(key, value)
    pipeline_db.set(id.text, log)
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key, 'log_id': id.text})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    """This function apparenlty boeit niet."""
    n = int(n)
    starting_money = int(starting_money)
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )
    
@app.get('/log/<log_id>')
def find_log(log_id: str):
    log_entry: LogUserValue = get_log_from_db(log_id)
    return jsonify(
        {
            "key": log_entry.key,
            "credit": log_entry.uservalue.credit,
            "dateTime": log_entry.dateTime
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    id = get_id()
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    log = msgpack.encode(LogUserValue(key=user_id, uservalue=user_entry, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")))
    pipeline_db.set(user_id, msgpack.encode(user_entry))
    pipeline_db.set(id.text, log)
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}, log_id: {id.text}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    id = get_id()
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    log = msgpack.encode(LogUserValue(key=user_id, uservalue=user_entry, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")))
    
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    pipeline_db.set(user_id, msgpack.encode(user_entry))
    pipeline_db.set(id.text, log)
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}, log_id: {id.text}", status=200)


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
