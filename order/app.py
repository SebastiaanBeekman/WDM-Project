import os
import logging
import atexit
import random
import uuid
import redis
import requests
from enum import Enum
from copy import deepcopy
from collections import defaultdict
from datetime import datetime, timedelta
from ast import literal_eval

from msgspec import msgpack, Struct
from flask import Flask, jsonify, abort, Response, request
from datetime import datetime

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

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


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


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


class LogOrderValue(Struct):
    id: str
    dateTime: str
    type: LogType | None = None
    status: LogStatus | None = None
    order_id: str | None = None
    old_ordervalue: OrderValue | None = None
    from_url: str | None = None
    to_url: str | None = None


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException as exc:
        abort(400, exc)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def get_order_from_db(order_id: str, log_id: str | None = None) -> OrderValue | None:
    try:
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    
    if entry is None:
        log_key = get_key()
        error_log = LogOrderValue(
            id=log_id if log_id else str(uuid.uuid4()),
            type=LogType.SENT,
            order_id=order_id,
            from_url=request.url,
            to_url=request.referrer,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
        )
        db.set(log_key, msgpack.encode(error_log))
        abort(400, f"Order: {order_id} not found! Log key: {log_key}")
    return entry

########################################################################################################################
#   START OF LOG FUNCTIONS
########################################################################################################################
def format_log_entry(log_entry: LogOrderValue) -> dict:
    return {
        "id": log_entry.id,
        "type": log_entry.type,
        "status": log_entry.status,
        "order_id": log_entry.order_id,
        "old_order_value": {
            "paid": log_entry.old_ordervalue.paid if log_entry.old_ordervalue else None,
            "items": log_entry.old_ordervalue.items if log_entry.old_ordervalue else None,
            "user_id": log_entry.old_ordervalue.user_id if log_entry.old_ordervalue else None,
            "total_cost": log_entry.old_ordervalue.total_cost if log_entry.old_ordervalue else None
        },
        "url": {
            "from": log_entry.from_url,
            "to": log_entry.to_url
        },
        "date_time": log_entry.dateTime,
    }
    

def sort_logs(logs: list[dict]):
    log_dict = defaultdict(list)
    for log in logs:
        log_dict[log["log"]["id"]].append(log)
    
    for key in log_dict:
        log_dict[key] = sorted(log_dict[key], key=lambda x: x["log"]["date_time"])
    
    return log_dict


def get_log_from_db(log_id: str) -> LogOrderValue | None:
    try:
        entry: bytes = db.get(log_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    entry: LogOrderValue | None = msgpack.decode(entry, type=LogOrderValue) if entry else None
    if entry is None:
        abort(400, f"Log: {log_id} not found!")
    return entry


@app.get('/log_count')
def get_log_count():
    return Response(str(len(db.keys("log:*"))), status=200)


@app.get('/log/<log_id>')
def find_log(log_id: str):
    log_entry: LogOrderValue = get_log_from_db(log_id)
    formatted_log_entry = format_log_entry(log_entry)
    return jsonify(formatted_log_entry), 200


@app.get('/logs')
def find_all_logs():
    try:
        # Retrieve all keys starting with "log:" from Redis
        log_keys = [key.decode('utf-8') for key in db.keys("log:*")]

        # Retrieve values corresponding to the keys
        logs = [{"id": key, "log": msgpack.decode(db.get(key))} for key in log_keys]

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

        # Filter keys based on the timestamp
        logs = []
        for key in potential_keys:
            timestamp_str = key.split(":")[-1][:20]
            key_timestamp: datetime = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S%f")
            
            if lower_bound > key_timestamp or upper_bound < key_timestamp:
                continue
            
            raw_data = db.get(key)
            
            if not raw_data:
                continue
            
            # Decode the raw data into a LogOrderValue object
            log_entry = msgpack.decode(raw_data, type=LogOrderValue)
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


# For testing purposes only
@app.post("/log/create")
def create_log():
    str_dict_log_entry = str(request.get_json())
    dict_log_entry = literal_eval(str_dict_log_entry)
    log_entry = LogOrderValue(**dict_log_entry)
    
    log_key = get_key()
    db.set(log_key, msgpack.encode(log_entry))
    
    return jsonify({"msg": "Log entry created", "log_key": log_key}), 200

########################################################################################################################
#   START OF BENCHMARK FUNCTIONS
########################################################################################################################
@app.post('/create/<user_id>/benchmark')
def create_order_benchmark(user_id: str):
    order_id = str(uuid.uuid4())
    order_value = OrderValue(user_id=user_id, total_cost=0, items=[], paid=False)
    
    try:
        db.set(order_id, msgpack.encode(order_value))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    return jsonify({"order_id": order_id}), 200


@app.get('/find/<order_id>/benchmark')
def find_order_benchmark(order_id: str):
    entry: OrderValue = db.get(order_id)
    order_entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    
    if order_entry is None:
        return abort(400, f"Order: {order_id} not found!")
    
    return jsonify({"user_id": order_entry.user_id, "total_cost": order_entry.total_cost, "items": order_entry.items, "paid": order_entry.paid}), 200


@app.post('/addItem/<order_id>/<item_id>/<quantity>/benchmark')
def add_item_benchmark(order_id: str, item_id: str, quantity: int):
    entry: OrderValue = db.get(order_id)
    order_entry = msgpack.decode(entry, type=OrderValue)
    
    item_json = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}/benchmark").json()
    
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}", status=200)


@app.post('/checkout/<order_id>/benchmark')
def checkout_benchmark(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():
        request_url = f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}"

        stock_reply = send_post_request(request_url)
        stock_reply_status = stock_reply.status_code

        if stock_reply_status != 200:
            rollback_stock(removed_items)
            return abort(400, f'Out of stock on item_id: {item_id}')
        
        removed_items.append((item_id, quantity))

    payment_request_url = f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}"

    payment_reply = send_post_request(payment_request_url)
    payment_reply_status = payment_reply.status_code

    if payment_reply_status != 200:
        rollback_stock(removed_items)    
        abort(400, "User out of credit")
    
    order_entry.paid = True

    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)

    return Response(f"Checkout successful", status=200)

########################################################################################################################
#   START OF MICROSERVICE FUNCTIONS
########################################################################################################################
@app.post('/create/<user_id>')
def create_order(user_id: str):
    log_id = str(uuid.uuid4())
    
    # Url for the request to the payment service
    request_url = f"{GATEWAY_URL}/payment/find_user/{user_id}"

    # Send request to the payment service to check if the user exists
    payment_reply = send_get_request(request_url)
    
    # Request failed because user does not exist
    if payment_reply.status_code != 200:        
        return abort(400, f"User: {user_id} does not exist!")
    
    # Create the order
    order_id = str(uuid.uuid4())
    order_value = OrderValue(
        paid=False, 
        items=[], 
        user_id=user_id, 
        total_cost=0
    )
    
    # Create a log entry for the create request
    create_payload = LogOrderValue(
        id=log_id,
        type=LogType.CREATE,
        order_id=order_id,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
    )
    
    # Set the log entry and the order value in the pipeline
    log_key = get_key()
    pipeline_db.set(log_key, msgpack.encode(create_payload))
    pipeline_db.set(order_id, msgpack.encode(order_value))
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        error_payload = LogOrderValue(
            id=log_id,
            type=LogType.SENT,
            from_url=request.url,
            to_url=request.referrer,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
        )
        db.set(get_key(), msgpack.encode(error_payload))
        
        pipeline_db.discard()
        
        return abort(400, DB_ERROR_STR)
     
    # Create a log for the sent to user response   
    sent_payload_to_user = LogOrderValue(
        id=log_id,
        type=LogType.SENT,
        from_url=request.url,       # This endpoint
        to_url=request.referrer,    # Endpoint that called this
        status=LogStatus.SUCCESS,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
    )
    db.set(get_key(), msgpack.encode(sent_payload_to_user))
    
    return jsonify({'order_id': order_id, 'log_id': log_id}), 200


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    
    # Return the order
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
        }
    ), 200


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    log_id = str(uuid.uuid4())

    # Url for the request to the stock service
    request_url = f"{GATEWAY_URL}/stock/find/{item_id}"

    # Send request to the stock service to check if the item exists
    stock_reply = send_get_request(request_url)

    # Request failed because item does not exist
    if stock_reply.status_code != 200:
        error_payload = LogOrderValue(
            id=log_id,
            type=LogType.SENT,
            from_url=request.url,
            to_url=request.referrer,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
        )
        db.set(get_key(), msgpack.encode(error_payload))
        return abort(400, f"Item: {item_id} does not exist!")

    # Get the order from the database and create a copy of it for rollback purposes
    order_entry: OrderValue = get_order_from_db(order_id)
    old_order_entry = deepcopy(order_entry)
    
    # Locally update the order locally
    item_json: dict = stock_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]

    # Create a log entry for the update request
    update_payload = LogOrderValue(
        id=log_id,
        type=LogType.UPDATE,
        order_id=order_id,
        old_ordervalue=old_order_entry,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
    )

    # Set the log entry and the updated order value in the pipeline
    pipeline_db.set(get_key(), msgpack.encode(update_payload))
    pipeline_db.set(order_id, msgpack.encode(order_entry))
    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        error_payload = LogOrderValue(
            id=log_id,
            type=LogType.SENT,
            from_url=request.url,
            to_url=request.referrer,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
        )
        db.set(get_key(), msgpack.encode(error_payload))
        
        pipeline_db.discard()
        
        return abort(400, DB_ERROR_STR)

    # Create a log for the sent response
    sent_payload_to_user = LogOrderValue(
        id=log_id,
        type=LogType.SENT,
        from_url=request.url,
        to_url=request.referrer,
        order_id=order_id,
        status=LogStatus.SUCCESS,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
    )
    db.set(get_key(), msgpack.encode(sent_payload_to_user))

    return Response(f"Item: {item_id} added to: {order_id} total price updated to: {order_entry.total_cost}, log_id: {log_id}", status=200)


def rollback_stock(removed_items: list[tuple[str, int]], log_id: str | None = None):    
    error_flag = False
    for item_id, quantity in removed_items:
        url = f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}"
        
        # Send request to the stock service to add the stock back
        rollback_resp = send_post_request(url)
        rollback_resp_status = rollback_resp.status_code
        
        # Create a log entry for the received response (success or failure)
        received_payload_from_stock = LogOrderValue(
            id=log_id,
            type=LogType.RECEIVED,
            from_url=url,
            to_url=request.url,
            status=LogStatus.SUCCESS if rollback_resp_status == 200 else LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
        )
        db.set(get_key(), msgpack.encode(received_payload_from_stock))
        
        # If one of the rollbacks failed, set the error flag to True
        if rollback_resp_status != 200: # No log on purpose since the fault tolerance should reroll again
            error_flag = True
    
    # If one of the rollbacks failed, return an error
    if error_flag:
        return abort(400, f"Failed to rollback")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}") # Keep this for benchmarking purposes
    
    log_id = str(uuid.uuid4())

    # Get the order from the database and create a copy of it for rollback purposes
    order_entry: OrderValue = get_order_from_db(order_id)
    old_order_entry = deepcopy(order_entry)

    # get the quantity per item
    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity

    # The removed items will contain the items that we already have successfully subtracted stock from for rollback purposes.
    removed_items: list[tuple[str, int]] = []
    for item_id, quantity in items_quantities.items():

        # Url for the request to the stock service
        request_url = f"{GATEWAY_URL}/stock/subtract/{item_id}/{quantity}"

        # Send request to the stock service to subtract the stock
        stock_reply = send_post_request(request_url)
        stock_reply_status = stock_reply.status_code

        # Create a log entry for the received response (success or failure)
        received_payload_from_stock = LogOrderValue(
            id=log_id,
            type=LogType.RECEIVED,
            from_url=request_url,
            to_url=request.url,
            status=LogStatus.SUCCESS if stock_reply_status == 200 else LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
        )
        db.set(get_key(), msgpack.encode(received_payload_from_stock))

        # If the stock request failed, rollback the stock, create a log, and return an error
        if stock_reply_status != 200:
            rollback_stock(removed_items, log_id)
            
            error_payload = LogOrderValue(
                id=log_id,
                type=LogType.SENT,
                from_url=request.url,
                to_url=request.referrer,
                status=LogStatus.FAILURE,
                dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
            )
            db.set(get_key(), msgpack.encode(error_payload))
            
            return abort(400, f'Out of stock on item_id: {item_id}')
        
        removed_items.append((item_id, quantity))

    # Url for the request to the payment service
    payment_request_url = f"{GATEWAY_URL}/payment/pay/{order_entry.user_id}/{order_entry.total_cost}"

    # Send request to the payment service to pay for the order
    payment_reply = send_post_request(payment_request_url)
    payment_reply_status = payment_reply.status_code

    # Create a log entry for the received response (success or failure)
    received_payload_from_payment = LogOrderValue(
        id=log_id,
        type=LogType.RECEIVED,
        from_url=payment_request_url,
        to_url=request.url,
        status=LogStatus.SUCCESS if payment_reply_status == 200 else LogStatus.FAILURE,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
    )
    db.set(get_key(), msgpack.encode(received_payload_from_payment))

    # If the payment request failed, rollback the stock, create a log, and return an error
    if payment_reply_status != 200:
        rollback_stock(removed_items, log_id)
        
        error_payload = LogOrderValue(
            id=log_id,
            type=LogType.SENT,
            from_url=request.url,
            to_url=request.referrer,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
        )
        db.set(get_key(), msgpack.encode(error_payload))
        
        abort(400, "User out of credit")

    # Locally update the order
    order_entry.paid = True
    
    # Create a log entry for the update request
    update_payload = LogOrderValue(
        id=log_id,
        type=LogType.UPDATE,
        order_id=order_id,
        old_ordervalue=old_order_entry,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
    )
    
    # Set the log entry and the updated order value in the pipeline
    log_key = get_key()
    pipeline_db.set(log_key, msgpack.encode(update_payload))
    pipeline_db.set(order_id, msgpack.encode(order_entry))

    try:
        pipeline_db.execute()
    except redis.exceptions.RedisError:
        error_payload = LogOrderValue(
            id=log_id,
            type=LogType.SENT,
            from_url=request.url,
            to_url=request.referrer,
            status=LogStatus.FAILURE,
            dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
        )
        db.set(get_key(), msgpack.encode(error_payload))
        
        pipeline_db.discard()
        
        return abort(400, DB_ERROR_STR)

    # Create a log for the sent response
    sent_payload_to_user = LogOrderValue(
        id=log_id,
        type=LogType.SENT,
        from_url=request.url,
        to_url=request.referrer,
        order_id=order_id,
        status=LogStatus.SUCCESS,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f"),
    )
    db.set(get_key(), msgpack.encode(sent_payload_to_user))

    app.logger.debug("Checkout successful") # Keep this for benchmarking purposes
    return Response(f"Checkout successful, log: {log_key}", status=200)

# Function to get an idempotent key from the ID service
def get_key():
    try:
        response = requests.get(f"{GATEWAY_URL}/ids/create")
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response.text

# Can ignore
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

# Function to call the fault tolerance function for testing purposes
@app.get('/fault_tolerance/<min_diff>')
def test_fault_tolerance(min_diff: int):
    fix_fault_tolerance(int(min_diff))
    return jsonify({"msg": "Fault Tollerance Successful"}), 200

# Fault tolerance function
def fix_fault_tolerance(min_diff: int = 5):
    time: datetime = datetime.now()
    logs = find_all_logs_time(time, int(min_diff))
    sorted_logs = sort_logs(logs)
    
    # Loop through log arrays with the same log_id
    for _, log_list in sorted_logs.items():
        last_log = log_list[-1]["log"]
        
        # Check if the last log was finished 'properly'
        if last_log["status"] in [LogStatus.SUCCESS, LogStatus.FAILURE] and last_log["type"] == LogType.SENT: # If log was finished properly
            continue
        
        # Check if the last log was a checkout log
        if last_log["url"]["from"] is not None and last_log["url"]["to"] is not None:
            if "checkout" in last_log["url"]["from"] or "checkout" in last_log["url"]["to"]: # If log was a checkout
                for log_entry in reversed(log_list):
                    log = log_entry["log"]
                    log_type = log["type"]
                    log_status = log["status"]
                    
                    # If the log was a checkout log and failed during the rollback, rollback the stock again
                    if (log_type == LogType.RECEIVED and log_status == LogStatus.FAILURE) and "stock/add" in log["url"]["from"]:
                        rollback_flag = True
                        rollback_url = GATEWAY_URL + "/stock/add/" + log["url"]["from"].split("add/")[1]
                        rollback_counter = 0
                        while rollback_flag:
                            try:
                                rollback_resp = requests.post(rollback_url)
                                if rollback_resp.status_code == 200:
                                    rollback_flag = False
                            except Exception as e:
                                app.logger.error(f"Rollback attempt failed: {e}")
                                if rollback_counter > 10:
                                    return abort(400, "Failed to rollback")
                                rollback_counter += 1
                    
                    db.delete(log_entry["id"])
                
                continue
        
        # If the last log was not finished properly, rollback the changes (if not checkout) and delete the logs
        for log_entry in reversed(log_list):
            log = log_entry["log"]
            
            log_type = log["type"]
            log_order_id = log["order_id"]
            if log_type == LogType.CREATE:
                db.delete(log_order_id)
            elif log_type == LogType.UPDATE:
                log_order_old = log["old_order_value"]
                db.set(log_order_id, msgpack.encode(OrderValue(paid=log_order_old["paid"], items=log_order_old["items"], user_id=log_order_old["user_id"], total_cost=log_order_old["total_cost"])))
            
            db.delete(log_entry["id"])
        

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    
    # app.logger.setLevel(logging.DEBUG)
    fix_fault_tolerance()
