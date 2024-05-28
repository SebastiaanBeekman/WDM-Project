import logging
import os
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
from datetime import datetime, timedelta


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("payment-service")

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


class UserValue(Struct):
    credit: int
    

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
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    
    if entry is None:
        log_key = get_key()
        error_payload = LogUserValue(
            id=log_id if log_id else str(uuid.uuid4()), 
            type=LogType.SENT, 
            user_id = user_id, 
            from_url = request.url, 
            to_url = request.referrer,
            status=LogStatus.FAILURE, 
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
        db.set(log_key, msgpack.encode(error_payload))
        abort(400, f"User: {user_id} not found! Log key: {log_key}")
    return entry

########################################################################################################################
#   START OF LOG FUNCTIONS
########################################################################################################################
def format_log_entry(log_entry: LogUserValue) -> dict:
    return {
        "id": log_entry.id,
        "type": log_entry.type,
        "status": log_entry.status,
        "user_id": log_entry.user_id,
        "user_value": {
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
        "date_time": log_entry.dateTime
    }
    
def sort_logs(logs: list[dict]):
    log_dict = defaultdict(list)
    for log in logs:
        log_dict[log["log"]["id"]].append(log)
    
    for key in log_dict:
        log_dict[key] = sorted(log_dict[key], key=lambda x: x["log"]["date_time"])
    
    return log_dict


def get_log_from_db(log_key: str) -> LogUserValue | None:
    try:
        entry: bytes = db.get(log_key)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    entry: LogUserValue | None = msgpack.decode(entry, type=LogUserValue) if entry else None
    
    if entry is None:
        abort(400, f"Log: {log_key} not found!")
    return entry


@app.get('/log_count')
def get_log_count():
    return Response(str(len(db.keys("log:*"))), status=200)


@app.get('/log/<log_key>')
def find_log(log_key: str):
    log_entry: LogUserValue = get_log_from_db(log_key)
    formatted_log = format_log_entry(log_entry)
    return jsonify(formatted_log), 200


@app.get('/logs')
def find_all_logs():
    try:
        # Retrieve all keys starting with "log:" from Redis
        log_keys = [key.decode('utf-8') for key in db.keys("log:*")]
        
        # Retrieve values corresponding to the keys
        logs = [{"key": key, "log": format_log_entry(msgpack.decode(db.get(key), type=LogUserValue))} for key in log_keys]

        return jsonify({'logs': logs}), 200
    except redis.exceptions.RedisError:
        return abort(500, 'Failed to retrieve logs from the database')
    

@app.get('/logs_from/<number>')
def find_all_logs_from(number: int):
    """This function is still broken."""
    try:
        # Retrieve all keys starting with "log:" from Redis
        log_keys = [key.decode('utf-8') for key in db.keys(f"log:{number}*")]
        
        # Retrieve values corresponding to the keys
        logs = [{"key": key, "log": msgpack.decode(db.get(key))} for key in log_keys]

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
                
            log_entry = msgpack.decode(raw_data, type=LogUserValue)
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
    log_entry = LogUserValue(**dict_log_entry)
    
    log_key = get_key()
    db.set(log_key, msgpack.encode(log_entry))
    
    return jsonify({"msg": "Log entry created", "log_key": log_key}), 200

########################################################################################################################
#   START OF BENCHMARK FUNCTIONS
########################################################################################################################
@app.post('/create_user/benchmark')
def create_user_benchmark():    
    user_id = str(uuid.uuid4())
    user_value = UserValue(credit=0)
    
    try:
        db.set(user_id, msgpack.encode(user_value))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    return jsonify({'user_id': user_id}), 200


@app.get('/find_user/<user_id>/benchmark')
def find_user_benchmark(user_id: str):
    entry: bytes = db.get(user_id)
    user_entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None

    if user_entry is None:
        return abort(400, f"Item: {user_id} not found!")

    return jsonify({"user_id": user_id, "credit": user_entry.credit}), 200


@app.post('/add_funds/<user_id>/<amount>/benchmark')
def add_credit_benchmark(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit += int(amount)
    
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    
    return jsonify({"credit": user_entry.credit}), 200

@app.post('/pay/<user_id>/<amount>/benchmark')
def remove_credit_benchmark(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    user_entry.credit -= int(amount)
    
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    
    try:
        db.set(user_id, msgpack.encode(user_entry))
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    
    return jsonify({"credit": user_entry.credit}), 200

########################################################################################################################
#   START OF MICROSERVICE FUNCTIONS
########################################################################################################################
@app.post('/create_user')
def create_user():
    log_id = str(uuid.uuid4())
    
    # create log entry for the received request
    received_payload_from_user = LogUserValue(
        id=log_id, 
        type=LogType.RECEIVED, 
        from_url = request.referrer,    # Endpoint that called this
        to_url = request.url,           # This endpoint
        status=LogStatus.PENDING, 
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(received_payload_from_user))
    
    user_id = str(uuid.uuid4())
    user_value = UserValue(credit=0)
    
    # Create a log entry for the create request
    create_payload = LogUserValue(
        id=log_id, 
        type=LogType.CREATE, 
        user_id = user_id, 
        new_uservalue=user_value, 
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    
    # Set the log entry and the updated item in the pipeline
    log_key = get_key()
    pipeline_db.set(log_key, msgpack.encode(create_payload))
    pipeline_db.set(user_id, msgpack.encode(user_value))
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    
    # create log entry for the sent response
    sent_payload_to_user = LogUserValue(
        id=log_id, 
        type=LogType.SENT, 
        from_url = request.url,     # This endpoint
        to_url = request.referrer,  # Endpoint that called this
        user_id = user_id, 
        status=LogStatus.SUCCESS, 
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
    db.set(get_key(), msgpack.encode(sent_payload_to_user))
    
    return jsonify({'user_id': user_id, 'log_key': log_key}), 200


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    log_id: str | None = request.args.get('log_id')
    log_id = log_id if log_id else str(uuid.uuid4())
    
    # create log entry for the received request from the user
    received_payload_from_user = LogUserValue(
        id=log_id, 
        type=LogType.RECEIVED, 
        from_url = request.referrer,    # Endpoint that called this
        to_url = request.url,           # This endpoint
        user_id = user_id, 
        status=LogStatus.PENDING, 
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(received_payload_from_user))
    
    # Retrieve user from the database
    user_entry: UserValue = get_user_from_db(user_id)
    
    # Create log entry for the sent response
    sent_payload_to_user = LogUserValue(
        id=log_id, 
        type=LogType.SENT, 
        from_url = request.url, 
        to_url = request.referrer, 
        user_id = user_id, 
        status=LogStatus.SUCCESS, 
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(sent_payload_to_user))
    
    # Return the user information
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
    
    # Create a log entry for the receieved request from the user
    received_payload_from_user = LogUserValue(
        id=log_id, 
        type=LogType.RECEIVED,
        status=LogStatus.PENDING,
        user_id = user_id,
        from_url = request.referrer,
        to_url = request.url,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(received_payload_from_user))
    
    user_entry: UserValue = get_user_from_db(user_id)
    old_user_entry: UserValue = deepcopy(user_entry)
    
    # Update credit
    user_entry.credit += int(amount)
    
    # Create a log entry for the updated user
    update_payload = LogUserValue(
        id=log_id, 
        type=LogType.UPDATE,
        user_id = user_id,
        old_uservalue=old_user_entry,
        new_uservalue=user_entry,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    
    # update database
    log_key = get_key()
    pipeline_db.set(log_key, msgpack.encode(update_payload))
    pipeline_db.set(user_id, msgpack.encode(user_entry))
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    
    # create log entry for the sent response
    sent_payload_to_user = LogUserValue(
        id=log_id, 
        type=LogType.SENT, 
        from_url = request.url,     # This endpoint
        to_url = request.referrer,  # Endpoint that called this
        user_id = user_id,
        status=LogStatus.SUCCESS, 
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(sent_payload_to_user))
    
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}, log_key: {log_key}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}") # Keep for benchmarking purposes
    log_id: str | None = request.args.get('log_id')
    log_id = log_id if log_id else str(uuid.uuid4())
    
    # create log entry for the received request
    received_payload_from_user = LogUserValue(
        id=log_id, 
        type=LogType.RECEIVED,
        from_url = request.referrer,    # Endpoint that called this
        to_url = request.url,           # This endpoint
        user_id = user_id,
        status=LogStatus.PENDING,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(received_payload_from_user))
    
    user_entry: UserValue = get_user_from_db(user_id)
    old_user_entry: UserValue = deepcopy(user_entry)
    
    # Update credit
    user_entry.credit -= int(amount)
    
    if user_entry.credit < 0:
        # create log entry for the error
        sent_log = LogUserValue(
            id=log_id, 
            type=LogType.SENT,
            status=LogStatus.FAILURE,
            old_uservalue=old_user_entry,
            new_uservalue=user_entry,
            user_id = user_id,
            from_url = request.url,
            to_url = request.referrer,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
        )
        log_key = get_key()
        db.set(log_key, msgpack.encode(sent_log))
        abort(400, f"User: {user_id} credit cannot get reduced below zero! Log key: {log_key}")
        
    # create log entry for the updated user
    update_payload = LogUserValue(
        id=log_id, 
        type=LogType.UPDATE,
        user_id=user_id,
        old_uservalue = old_user_entry,
        new_uservalue=user_entry,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    
    # Set the log entry and the updated item in the pipeline
    log_key = get_key()
    pipeline_db.set(log_key, msgpack.encode(update_payload))
    pipeline_db.set(user_id, msgpack.encode(user_entry))
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        pipeline_db.discard()
        return abort(400, DB_ERROR_STR)
    
    # Create log entry for the sent response
    sent_payload_to_user = LogUserValue(
        id=log_id, 
        type=LogType.SENT,
        from_url = request.url,     # This endpoint
        to_url = request.referrer,  # Endpoint that called this
        user_id = user_id,
        status=LogStatus.SUCCESS,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    db.set(get_key(), msgpack.encode(sent_payload_to_user))
    
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}, log_key: {log_key}", status=200)


def get_key():
    try:
        response = requests.get(f"{GATEWAY_URL}/ids/create")
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response.text


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


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
