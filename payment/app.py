import logging
import os
import atexit
import uuid
import requests
from datetime import datetime
from enum import Enum

import redis

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request

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
    

class LogType(str, Enum):
    CREATE = "Create"
    UPDATE = "Update"
    DELETE = "Delete"
    SENT = "Sent"
    RECEIVED = "Received"


class LogStatus(str, Enum):
    PENDING = "Pending"
    SUCCESS = "Success"
    FAILURE = "Failure"


class LogUserValue(Struct):
    id: str
    dateTime: str
    type: LogType | None = None 
    status: LogStatus | None = None
    user_id: str | None = None
    old_uservalue: UserValue | None = None
    new_uservalue: UserValue | None = None
    from_url: str | None = None
    to_url: str | None = None
    
    

def get_user_from_db(user_id: str, log_id: str | None = None) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        log_send = LogUserValue(id=log_id if log_id else str(uuid.uuid4()), type=LogType.SENT, status=LogStatus.FAILURE, user_id = user_id, from_url = request.url, to_url = request.referrer, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry


def format_log_entry(log_entry: LogUserValue) -> dict:
    return {
        "id": log_entry.id,
        "type": log_entry.type,
        "status": log_entry.status,
        "user_id": log_entry.user_id,
        "uservalue": {
            "old": {
                "credit": log_entry.old_uservalue.credit if log_entry.old_uservalue else None
            },
            "new": {
                "credit": log_entry.new_uservalue.credit if log_entry.new_uservalue else None
            }
        },
        "url": {
            "from": log_entry.from_url,
            "to": log_entry.to_url
        },
        "dateTime": log_entry.dateTime
    }
    

def get_log_from_db(log_key: str) -> LogUserValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(log_key)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: LogUserValue | None = msgpack.decode(entry, type=LogUserValue) if entry else None
    if entry is None:
        abort(400, f"Log: {log_key} not found!")
    return entry


@app.get('/log/<log_key>')
def find_log(log_key: str):
    log_entry: LogUserValue = get_log_from_db(log_key)
    formatted_log = format_log_entry(log_entry)
    return jsonify(formatted_log)


@app.get('/logs')
def get_all_logs():
    try:
        # Retrieve all keys starting with "log:" from Redis
        log_keys = [key.decode('utf-8') for key in db.keys("log:*")]
        
        # Retrieve values corresponding to the keys
        logs = [{"key": key, "log": format_log_entry(msgpack.decode(db.get(key), type=LogUserValue))} for key in log_keys]

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
        logs = [{"key": key, "log": msgpack.decode(db.get(key))} for key in log_keys]

        return jsonify({'logs': logs}), 200
    except redis.exceptions.RedisError:
        return abort(500, 'Failed to retrieve logs from the database')


@app.post('/create_user')
def create_user():
    user_id = str(uuid.uuid4())
    log_id = str(uuid.uuid4())
    
    # create log entry for the received request
    recv_log = LogUserValue(id=log_id, type=LogType.RECEIVED, status=LogStatus.PENDING, from_url = request.referrer, to_url = request.url, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
    db.set(get_key(), msgpack.encode(recv_log))
    
    uservalue = UserValue(credit=0)
    value = msgpack.encode(uservalue)
    
    # create log entry for the created user
    log = LogUserValue(id=log_id, type=LogType.CREATE, new_uservalue=uservalue, user_id = user_id, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
    
    # update database
    log_key = get_key()
    pipeline_db.set(log_key, msgpack.encode(log))
    pipeline_db.set(user_id, value)
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    
    # create log entry for the sent response
    send_log = LogUserValue(id=log_id, type=LogType.SENT, status=LogStatus.SUCCESS, user_id = user_id, from_url = request.url, to_url = request.referrer, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
    db.set(get_key(), msgpack.encode(send_log))
    
    return jsonify({'user_id': user_id, 'log_key': log_key})


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
    log_id: str | None = request.args.get('log_id')
    log_id = log_id if log_id else str(uuid.uuid4())
    
    # create log entry for the received request
    recv_log = LogUserValue(id=log_id, type=LogType.RECEIVED, status=LogStatus.PENDING, user_id = user_id, from_url = request.referrer, to_url = request.url, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
    db.set(get_key(), msgpack.encode(recv_log))
    
    # get user from database
    user_entry: UserValue = get_user_from_db(user_id)
    
    # Create log entry for the sent response
    sent_log = LogUserValue(id=log_id, type=LogType.SENT, status=LogStatus.SUCCESS, user_id = user_id, from_url = request.url, to_url = request.referrer, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
    db.set(get_key(), msgpack.encode(sent_log))
    
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )
    

@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    log_id: str | None = request.args.get('log_id')
    log_id = log_id if log_id else str(uuid.uuid4())
    
    # create log entry for the received request
    recv_log = LogUserValue(id=log_id, type=LogType.RECEIVED, status=LogStatus.PENDING, user_id = user_id, from_url = request.referrer, to_url = request.url, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
    db.set(get_key(), msgpack.encode(recv_log))
    
    user_entry: UserValue = get_user_from_db(user_id)
    old_user = UserValue(credit=user_entry.credit)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    
    # create log entry for the updated user
    log = LogUserValue(id=log_id, type=LogType.UPDATE, old_uservalue=old_user, new_uservalue=user_entry, user_id = user_id, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
    
    # update database
    pipeline_db.set(user_id, msgpack.encode(user_entry))
    log_key = get_key()
    pipeline_db.set(log_key, msgpack.encode(log))
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    
    # create log entry for the sent response
    send_log = LogUserValue(id=log_id, type=LogType.SENT, status=LogStatus.SUCCESS, user_id = user_id, from_url = request.url, to_url = request.referrer, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
    db.set(get_key(), msgpack.encode(send_log))
    
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}, log_key: {log_key}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    log_id: str | None = request.args.get('log_id')
    log_id = log_id if log_id else str(uuid.uuid4())
    
    # create log entry for the received request
    recv_log = LogUserValue(id=log_id, type=LogType.RECEIVED, status=LogStatus.PENDING, user_id = user_id, from_url = request.referrer, to_url = request.url, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
    db.set(get_key(), msgpack.encode(recv_log))
    
    user_entry: UserValue = get_user_from_db(user_id)
    old_user = UserValue(credit=user_entry.credit)
    # update credit, serialize and update database
    user_entry.credit -= int(amount)
    
    if user_entry.credit < 0:
        # create log entry for the sent response
        send_log = LogUserValue(id=log_id, type=LogType.SENT, status=LogStatus.FAILURE, old_uservalue=old_user, new_uservalue=user_entry, user_id = user_id, from_url = request.url, to_url = request.referrer, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
        log_key = get_key()
        db.set(log_key, msgpack.encode(send_log))
        abort(400, f"User: {user_id} credit cannot get reduced below zero!, log_key: {log_key}")
        
    # create log entry for the updated user
    log = LogUserValue(id=log_id, type=LogType.UPDATE, old_uservalue = old_user, new_uservalue=user_entry, user_id=user_id, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
    
    # update database
    pipeline_db.set(user_id, msgpack.encode(user_entry))
    log_key = get_key()
    pipeline_db.set(log_key, msgpack.encode(log))
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    
    # Create log entry for the sent response
    send_log = LogUserValue(id=log_id, type=LogType.SENT, status=LogStatus.SUCCESS, user_id = user_id, from_url = request.url, to_url = request.referrer, dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"))
    db.set(get_key(), msgpack.encode(send_log))
    
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}, log_key: {log_key}", status=200)


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
