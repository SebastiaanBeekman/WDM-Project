import requests
from msgspec import msgpack

from datetime import datetime
from class_utils import (
    LogStockValue, LogType, LogStatus, StockValue
)

ORDER_URL = STOCK_URL = PAYMENT_URL = IDS_URL = "http://127.0.0.1:8000"

def get_key():
    return requests.get(f"{IDS_URL}/ids/create").text

########################################################################################################################
#   LOGGING FUNCTIONS, I guess not really anymore sorry
########################################################################################################################
def create_received_from_user_log(log_id: str):
    return LogStockValue(
        id=log_id,
        type=LogType.RECEIVED,
        status=LogStatus.PENDING,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    
    # return requests.post(f"{STOCK_URL}/stock/log/create/{log_id}").json()
    
    # db.set(get_key(), msgpack.encode(received_payload_from_user))
    # Send reqeust to microservice with data
    
def create_item_replacement(item_id: str, stock_value: StockValue, log_id: str):
    # Create a log entry for the create request
    create_payload = LogStockValue(
        id=log_id,
        type=LogType.CREATE,
        stock_id=item_id,
        new_stockvalue=stock_value,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
    


########################################################################################################################
#   STOCK MICROSERVICE FUNCTIONS
########################################################################################################################
def create_item(price: int) -> dict:
    return requests.post(f"{STOCK_URL}/stock/item/create/{price}").json()


def find_item(item_id: str) -> dict:
    return requests.get(f"{STOCK_URL}/stock/find/{item_id}").json()


def add_stock(item_id: str, amount: int) -> int:
    return requests.post(f"{STOCK_URL}/stock/add/{item_id}/{amount}").status_code


def subtract_stock(item_id: str, amount: int) -> int:
    return requests.post(f"{STOCK_URL}/stock/subtract/{item_id}/{amount}").status_code

def get_stock_log_count() -> dict:
    return requests.get(f"{STOCK_URL}/stock/log_count").json()

def get_stock_log() -> dict:
    return requests.get(f"{STOCK_URL}/stock/sorted_logs/1").json()

def send_anything_stock(id, anything):
    return requests.post(f"{STOCK_URL}/stock/put_anything/{id}/{anything}")

def get_anything_stock(id):
    return requests.get(f"{STOCK_URL}/stock/get_anything/{id}").json()["msg"]

def fault_tolerance_stock():
    return requests.get(f"{STOCK_URL}/stock/fault_tollerance/1")


########################################################################################################################
#   PAYMENT MICROSERVICE FUNCTIONS
########################################################################################################################
def payment_pay(user_id: str, amount: int) -> int:
    return requests.post(f"{PAYMENT_URL}/payment/pay/{user_id}/{amount}").status_code


def create_user() -> dict:
    return requests.post(f"{PAYMENT_URL}/payment/create_user").json()


def find_user(user_id: str) -> dict:
    return requests.get(f"{PAYMENT_URL}/payment/find_user/{user_id}").json()


def add_credit_to_user(user_id: str, amount: float) -> int:
    return requests.post(f"{PAYMENT_URL}/payment/add_funds/{user_id}/{amount}").status_code


########################################################################################################################
#   ORDER MICROSERVICE FUNCTIONS
########################################################################################################################
def create_order(user_id: str) -> dict:
    return requests.post(f"{ORDER_URL}/orders/create/{user_id}").json()


def add_item_to_order(order_id: str, item_id: str, quantity: int) -> int:
    return requests.post(f"{ORDER_URL}/orders/addItem/{order_id}/{item_id}/{quantity}").status_code


def find_order(order_id: str) -> dict:
    return requests.get(f"{ORDER_URL}/orders/find/{order_id}").json()


def checkout_order(order_id: str) -> requests.Response:
    return requests.post(f"{ORDER_URL}/orders/checkout/{order_id}")


########################################################################################################################
#   STATUS CHECKS
########################################################################################################################
def status_code_is_success(status_code: int) -> bool:
    return 200 <= status_code < 300


def status_code_is_failure(status_code: int) -> bool:
    return 400 <= status_code < 500
