import unittest

import uuid
import utils as tu
from class_utils import LogType, LogStatus, OrderValue

class TestMicroservices(unittest.TestCase):

    def test_order_contains_no_faulty_logs(self):
        # Get initial log count
        order_log_count = int(tu.get_order_log_count())
        
        # Create a user
        user_id = tu.create_user_benchmark().json()
        self.assertIn('user_id', user_id)
        
        # Add credit
        credit_added = tu.add_credit_to_user_benchmark(user_id['user_id'], 100).json()
        self.assertIn('credit', credit_added)
        
        # Test /order/create
        order1: dict = tu.create_order(user_id['user_id'])
        self.assertIn('order_id', order1)
        self.assertIn('log_id', order1)
        
        # Check if log count increased by 5
        order_log_count += 5
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
        self.assertTrue(tu.status_code_is_success(checkout_response))
        
        order_log_count += 7
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        # Check if last log is correct
        order_log = tu.get_order_log()
        last_checkout_log = order_log[order1['log_id']][-1]["log"]
        self.assertEqual(last_checkout_log['type'], "Sent")
        self.assertEqual(last_checkout_log["status"], "Success")
        
        tu.fault_tolerance_order()
        
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        
    def test_order_create_contains_faulty_log(self):
        # Get initial log count
        order_log_count = int(tu.get_order_log_count())
        self.assertIsNotNone(order_log_count)
        # print(order_log_count)
        
        for i in range(2):
            log_id = str(uuid.uuid4())
            
            # Create a user
            user_id = tu.create_user_benchmark().json()
            self.assertIn('user_id', user_id)
            
            endpoint_url = f"{tu.ORDER_URL}/orders/create/{user_id['user_id']}"

            # # Create an entry for the receive from user log
            if i >= 0:
                log1_resp = tu.create_order_log(
                    log_id=log_id,
                    type=LogType.RECEIVED,
                    from_url="BENCHMARK",
                    to_url=endpoint_url,
                    status=LogStatus.PENDING,
                )
                self.assertTrue(tu.status_code_is_success(log1_resp.status_code))
                
                order_log_count += 1
                self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
            # Create an order with given user
            if i >= 1:
                order1_resp = tu.create_order_benchmark(user_id['user_id'])
                self.assertTrue(tu.status_code_is_success(order1_resp.status_code))
                
                order1_id = order1_resp.json()['order_id']
                order_value = OrderValue(user_id=user_id['user_id'], paid=False, items=[], total_cost=0)
                
                find_order1_resp = tu.find_order_benchmark(order1_id)
                self.assertTrue(tu.status_code_is_success(find_order1_resp.status_code))
                self.assertEqual(find_order1_resp.json()['user_id'], user_id['user_id'])
            
            # Create an entry for the create log
            if i >= 1:
                log3_resp = tu.create_order_log(
                    log_id=log_id,
                    type=LogType.CREATE,
                    order_id=order1_id,
                    new_ordervalue=order_value,
                )
                self.assertTrue(tu.status_code_is_success(log3_resp.status_code))
                
                order_log_count += 1
                self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
            
            ft_resp = tu.fault_tolerance_order()
            # print(tu.get_order_log_count())
            self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
            
            order_log_count -= i+1
            self.assertEqual(tu.get_order_log_count(), order_log_count)
            
            # Check whether item was deleted
            if i >= 1:
                find_order1_resp = tu.find_item_benchmark(order1_id)
                self.assertTrue(tu.status_code_is_failure(find_order1_resp.status_code))



        
        
if __name__ == '__main__':
    unittest.main()