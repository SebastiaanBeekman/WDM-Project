import requests

from datetime import datetime
from class_utils import (
    LogStockValue, LogType, LogStatus, StockValue
)

ORDER_URL = STOCK_URL = PAYMENT_URL = IDS_URL = "http://127.0.0.1:8000"

def get_key():
    return requests.get(f"{IDS_URL}/ids/create").text

########################################################################################################################
#   LOGGING FUNCTIONS
########################################################################################################################
def create_stock_log(log_id: int, type: LogType, status: LogStatus = None, stock_id: str = None, old_stockvalue: StockValue = None, new_stockvalue: StockValue = None, from_url: str = None, to_url: str = None):
    log_entry = LogStockValue(
        id=log_id,
        type=type if type else None,
        status=status if status else None,
        stock_id=stock_id if stock_id else None,
        old_stockvalue=old_stockvalue if old_stockvalue else None,
        new_stockvalue=new_stockvalue if new_stockvalue else None,
        from_url=from_url if from_url else None,
        to_url=to_url if to_url else None,
        dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
    )
   
    return requests.post(f"{STOCK_URL}/stock/log/create", json=log_entry.to_dict())

########################################################################################################################
#   STOCK MICROSERVICE FUNCTIONS
########################################################################################################################
def create_item_benchmark(price: int) -> dict:
    return requests.post(f"{STOCK_URL}/stock/item/create/{price}/benchmark")


def create_item(price: int) -> dict:
    return requests.post(f"{STOCK_URL}/stock/item/create/{price}").json()


def find_item_benchmark(item_id: str) -> dict:
    return requests.get(f"{STOCK_URL}/stock/find/{item_id}/benchmark")


def find_item(item_id: str) -> dict:
    return requests.get(f"{STOCK_URL}/stock/find/{item_id}").json()


def add_stock_benchmark(item_id: str, amount: int) -> int:
    return requests.post(f"{STOCK_URL}/stock/add/{item_id}/{amount}/benchmark")


def add_stock(item_id: str, amount: int) -> int:
    return requests.post(f"{STOCK_URL}/stock/add/{item_id}/{amount}").status_code


def subtract_stock_benchmark(item_id: str, amount: int) -> int:
    return requests.post(f"{STOCK_URL}/stock/subtract/{item_id}/{amount}/benchmark")


def subtract_stock(item_id: str, amount: int) -> int:
    return requests.post(f"{STOCK_URL}/stock/subtract/{item_id}/{amount}").status_code


def get_stock_log_count() -> dict:
    return requests.get(f"{STOCK_URL}/stock/log_count").json()


def get_stock_log() -> dict:
    return requests.get(f"{STOCK_URL}/stock/sorted_logs/1").json()


def fault_tolerance_stock():
    return requests.get(f"{STOCK_URL}/stock/fault_tollerance/1")

########################################################################################################################
#   PAYMENT MICROSERVICE FUNCTIONS
########################################################################################################################
def payment_pay(user_id: str, amount: int) -> int:
    return requests.post(f"{PAYMENT_URL}/payment/pay/{user_id}/{amount}").status_code


def payment_pay_benchmark(user_id: str, amount: int) -> int:
    return requests.post(f"{PAYMENT_URL}/payment/pay/{user_id}/{amount}/benchmark")


def create_user() -> dict:
    return requests.post(f"{PAYMENT_URL}/payment/create_user").json()


def create_user_benchmark() -> dict:
    return requests.post(f"{PAYMENT_URL}/payment/create_user/benchmark")


def find_user(user_id: str) -> dict:
    return requests.get(f"{PAYMENT_URL}/payment/find_user/{user_id}").json()


def find_user_benchmark(user_id: str) -> dict:
    return requests.get(f"{PAYMENT_URL}/payment/find_user/{user_id}/benchmark")


def add_credit_to_user(user_id: str, amount: float) -> int:
    return requests.post(f"{PAYMENT_URL}/payment/add_funds/{user_id}/{amount}").status_code


def add_credit_to_user_benchmark(user_id: str, amount: float) -> int:
    return requests.post(f"{PAYMENT_URL}/payment/add_funds/{user_id}/{amount}/benchmark")

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
