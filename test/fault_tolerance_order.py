import unittest

import uuid
import utils as tu
from class_utils import LogType, LogStatus, StockValue

class TestMicroservices(unittest.TestCase):

    def test_order_contains_no_faulty_logs(self):
        # Get initial log count
        order_log_count = int(tu.get_order_log_count())
        
        # Create a user
        user_id = tu.create_user_benchmark().json()
        self.assertIn('user_id', user_id)
        
        # Test /order/create
        order1: dict = tu.create_order(user_id)
        self.assertIn('order_id', order1)
        self.assertIn('log_id', order1)
        
        # Check if log count increased by 3
        order_log_count += 3
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        # Check if last log is correct
        order_log = tu.get_order_log()
        last_create_log = order_log[order1['log_id']][-1]["log"]
        self.assertEqual(last_create_log['type'], "Sent")
        self.assertEqual(last_create_log["status"], "Success")
        
        # Test /order/find
        order2 = tu.find_order(order1['order_id'])
        self.assertIn('paid', order2)
        
        # Check if log count increased by 2
        order_log_count += 2
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        # Check if last log is correct
        order_log = tu.get_order_log()
        last_find_log = order_log[order2['log_id']][-1]["log"]
        self.assertEqual(last_find_log['type'], "Sent")
        self.assertEqual(last_find_log["status"], "Success")
        
        # Create an item
        item1 = tu.create_item_benchmark(2).json()
        self.assertIn("item_id", item1)
        
        # Add stock to item
        stock_added = tu.add_stock_benchmark(item1['item_id'], 20).json()
        self.assertIn("stock", stock_added)
        
        # Test /order/add/item
        add_item_to_order_response = tu.add_item_to_order(order1['order_id'] ,item1['item_id'], 3)
        print(add_item_to_order_response)
        self.assertTrue(tu.status_code_is_success(add_item_to_order_response))
        
        # Check if log count increased by 5
        order_log_count += 5
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        # # Check if last log is correct
        order_log = tu.get_order_log()
        last_add_log = order_log[order1['log_id']][-1]["log"]
        self.assertEqual(last_add_log['type'], "Sent")
        self.assertEqual(last_add_log["status"], "Success")
        
        
        # Test checkout
        checkout_response = tu.checkout_order(order1['order_id']).status_code
        print(checkout_response)
        self.assertTrue(tu.status_code_is_success(checkout_response))
        
        # # Test /stock/subtract
        # subtract_stock_response = tu.subtract_stock(item1['item_id'], 15)
        # self.assertTrue(tu.status_code_is_success(subtract_stock_response))
        
        # # Check if log count increased by 3
        # stock_log_count += 3
        # self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
        
        # # Check if last log is correct
        # stock_log = tu.get_stock_log()
        # last_subtract_log = stock_log[item1['log_id']][-1]["log"]
        # self.assertEqual(last_subtract_log['type'], "Sent")
        # self.assertEqual(last_subtract_log["status"], "Success")
        
        
if __name__ == '__main__':
    unittest.main()