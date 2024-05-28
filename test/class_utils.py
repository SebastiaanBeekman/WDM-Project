from msgspec import Struct
from enum import Enum


########################################################################################################################
#   STOCK MICROSERVICE DATA STRUCTURES
########################################################################################################################
class StockValue(Struct):
    stock: int
    price: int
    
    def to_dict(self):
        return {f: getattr(self, f) for f in self.__struct_fields__}


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
    
    def to_dict(self):
        result = {}
        for f in self.__struct_fields__:
            value = getattr(self, f)
            if isinstance(value, Struct):
                result[f] = value.to_dict()
            else:
                result[f] = value
        return result