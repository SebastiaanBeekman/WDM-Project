import unittest

import uuid
import utils as tu
from class_utils import LogStockValue, LogType, LogStatus, StockValue
from datetime import datetime
from msgspec import msgpack


class TestMicroservices(unittest.TestCase):
    
    def test_stock_contains_no_faulty_logs(self):
        # Get initial log count
        stock_log_count = int(tu.get_stock_log_count())
        
        # Test /stock/create
        item1: dict = tu.create_item(5)
        self.assertIn('item_id', item1)
        self.assertIn('log_id', item1)
        
        # Check if log count increased by 3
        stock_log_count += 3
        self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
        
        # Check if last log is correct
        stock_log = tu.get_stock_log()
        last_create_log = stock_log[item1['log_id']][-1]["log"]
        self.assertEqual(last_create_log['type'], "Sent")
        self.assertEqual(last_create_log["status"], "Success")
        
        # Test /stock/find
        item2 = tu.find_item(item1['item_id'])
        self.assertIn('price', item2)
        
        # Check if log count increased by 2
        stock_log_count += 2
        self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
        
        # Check if last log is correct
        stock_log = tu.get_stock_log()
        last_find_log = stock_log[item2['log_id']][-1]["log"]
        self.assertEqual(last_find_log['type'], "Sent")
        self.assertEqual(last_find_log["status"], "Success")
        
        # Test /stock/add
        add_stock_response = tu.add_stock(item1['item_id'], 15)
        self.assertTrue(tu.status_code_is_success(add_stock_response))
        
        # Check if log count increased by 3
        stock_log_count += 3
        self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
        
        # Check if last log is correct
        stock_log = tu.get_stock_log()
        last_add_log = stock_log[item1['log_id']][-1]["log"]
        self.assertEqual(last_add_log['type'], "Sent")
        self.assertEqual(last_add_log["status"], "Success")
        
        # Test /stock/subtract
        subtract_stock_response = tu.subtract_stock(item1['item_id'], 15)
        self.assertTrue(tu.status_code_is_success(subtract_stock_response))
        
        # Check if log count increased by 3
        stock_log_count += 3
        self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
        
        # Check if last log is correct
        stock_log = tu.get_stock_log()
        last_subtract_log = stock_log[item1['log_id']][-1]["log"]
        self.assertEqual(last_subtract_log['type'], "Sent")
        self.assertEqual(last_subtract_log["status"], "Success")
        

    def test_stock_create_contains_faulty_log(self):
        # Get initial log count
        stock_log_count = int(tu.get_stock_log_count())
        
        for i in range(4):
            
            log_id = str(uuid.uuid4())

            # Create a log entry for the receive request
            log_key_receive = tu.get_key()
            received_payload_from_user = tu.create_received_from_user_log(log_id)
            tu.send_anything(log_key_receive, received_payload_from_user)
            stock_log_count += 1
            
            # Fault Tollerance: CRASH - Undo
            if i == 0:
                self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
                

            # if i >= 1:
            #     item_id = str(uuid.uuid4())
            #     stock_value = StockValue(stock = 0, price = int(2))

            #     # Set the log entry and the updated item in the pipeline
            #     log_key_create = tu.get_key()
                
            #     tu.create_item_replacement(log_key_create, item_id, stock_value, log_id)
            #     stock_log_count += 1
            




if __name__ == '__main__':
    unittest.main()
