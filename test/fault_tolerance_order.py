import unittest

import uuid
import utils as tu
from class_utils import LogType, LogStatus, StockValue

class TestMicroservices(unittest.TestCase):

    def test_order_contains_no_faulty_logs(self):
        # Get initial log count
        order_log_count = int(tu.get_order_log_count())
        
        # Create a user
        user_id = tu.create_user_benchmark()
        self.assertIn('user_id', user_id)
        
        # Test /order/create
        order1: dict = tu.create_order(user_id)
        self.assertIn('order_id', order1)
        self.assertIn('log_id', order1)
        
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
        
        
if __name__ == '__main__':
    unittest.main()