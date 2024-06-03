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
        
        # Check if log count increased by 2
        order_log_count += 2
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        # Check if last log is correct
        order_log = tu.get_order_log()
        last_create_log = order_log[order1['log_id']][-1]["log"]
        self.assertEqual(last_create_log['type'], "Sent")
        self.assertEqual(last_create_log["status"], "Success")
        
        # Test /order/create with faulty log
        log1_id = str(uuid.uuid4())
        
        # Create an entry for the Redis error log
        log1_resp = tu.create_order_log(
            log_id=log1_id,
            type=LogType.SENT,
            order_id=order1['order_id'],
            status=LogStatus.FAILURE,
        )
        self.assertTrue(tu.status_code_is_success(log1_resp.status_code))
        
        order_log_count += 1
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        # Run fault tolerance
        ft_resp = tu.fault_tolerance_order()
        self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
        
        # Check if log count does not change
        self.assertEqual(tu.get_order_log_count(), order_log_count)
        
        # Test /order/find
        order2 = tu.find_order(order1['order_id'])
        self.assertIn('paid', order2)
        
        # Check if log count did not increase
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        # Create an item
        item1 = tu.create_item_benchmark(2).json()
        self.assertIn("item_id", item1)
        
        # Add stock to item
        stock_added = tu.add_stock_benchmark(item1['item_id'], 20).json()
        self.assertIn("stock", stock_added)
        
        # Test /order/add/item
        add_item_to_order_response = tu.add_item_to_order(order1['order_id'], item1['item_id'], 3)
        self.assertTrue(tu.status_code_is_success(add_item_to_order_response))
        
        # Check if log count increased by 2
        order_log_count += 2
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        # # Check if last log is correct
        order_log = tu.get_order_log()
        last_add_log = order_log[order1['log_id']][-1]["log"]
        self.assertEqual(last_add_log['type'], "Sent")
        self.assertEqual(last_add_log["status"], "Success")
        
        # Test /order/add/item with faulty log
        log2_id = str(uuid.uuid4())
        
        # Create an entry for the Redis error log
        log2_resp = tu.create_order_log(
            log_id=log2_id,
            type=LogType.SENT,
            order_id=order1['order_id'],
            status=LogStatus.FAILURE,
        )
        self.assertTrue(tu.status_code_is_success(log2_resp.status_code))
        
        order_log_count += 1
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        # Run fault tolerance
        ft_resp = tu.fault_tolerance_order()
        self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
        
        # Check if log count does not change
        self.assertEqual(tu.get_order_log_count(), order_log_count)
        
        # Test /checkout
        checkout_response = tu.checkout_order(order1['order_id']).status_code
        self.assertTrue(tu.status_code_is_success(checkout_response))
        
        order_log_count += 4
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        # Check if last log is correct
        order_log = tu.get_order_log()
        last_checkout_log = order_log[order1['log_id']][-1]["log"]
        self.assertEqual(last_checkout_log['type'], "Sent")
        self.assertEqual(last_checkout_log["status"], "Success")
        
        # Test /checkout with faulty log (stock)
        log3_id = str(uuid.uuid4())
        
        # Create an entry for the Redis error log
        log3_resp = tu.create_order_log(
            log_id=log3_id,
            type=LogType.SENT,
            order_id=order1['order_id'],
            status=LogStatus.FAILURE,
        )
        self.assertTrue(tu.status_code_is_success(log3_resp.status_code))
        
        order_log_count += 1
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        # Run fault tolerance
        ft_resp = tu.fault_tolerance_order()
        self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
        
        # Check if log count does not change
        self.assertEqual(tu.get_order_log_count(), order_log_count)
        
        # Test /checkout with faulty log (payment)
        log4_id = str(uuid.uuid4())
        
        # Create an entry for the Redis error log
        log4_resp = tu.create_order_log(
            log_id=log4_id,
            type=LogType.SENT,
            order_id=order1['order_id'],
            status=LogStatus.FAILURE,
        )
        self.assertTrue(tu.status_code_is_success(log4_resp.status_code))
        
        order_log_count += 1
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        # Run fault tolerance
        ft_resp = tu.fault_tolerance_order()
        self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
        
        # Check if log count does not change
        self.assertEqual(tu.get_order_log_count(), order_log_count)
        
        
    def test_order_create_contains_faulty_log(self):
        # Get initial log count
        order_log_count = int(tu.get_order_log_count())
        self.assertIsNotNone(order_log_count)
        
        log_id = str(uuid.uuid4())
            
        # Create a user
        user_id = tu.create_user_benchmark().json()
        self.assertIn('user_id', user_id)
        
        # Create an order with given user
        order1_resp = tu.create_order_benchmark(user_id['user_id'])
        self.assertTrue(tu.status_code_is_success(order1_resp.status_code))
        
        order1_id = order1_resp.json()['order_id']
       
        find_order1_resp = tu.find_order_benchmark(order1_id)
        self.assertTrue(tu.status_code_is_success(find_order1_resp.status_code))
        self.assertEqual(find_order1_resp.json()['user_id'], user_id['user_id'])
        
        # Create an entry for the create log
        log3_resp = tu.create_order_log(
            log_id=log_id,
            type=LogType.CREATE,
            order_id=order1_id
        )
        self.assertTrue(tu.status_code_is_success(log3_resp.status_code))
        
        order_log_count += 1
        self.assertEqual(int(tu.get_order_log_count()), order_log_count)
        
        ft_resp = tu.fault_tolerance_order()
        # print(tu.get_order_log_count())
        self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
        
        order_log_count -= 1
        self.assertEqual(tu.get_order_log_count(), order_log_count)
        
        # Check whether item was deleted
        find_order1_resp = tu.find_item_benchmark(order1_id)
        self.assertTrue(tu.status_code_is_failure(find_order1_resp.status_code))

    
    # def test_order_add_item_contains_faulty_log(self):
    #     # Get initial log count
    #     order_log_count = int(tu.get_order_log_count())
    #     self.assertIsNotNone(order_log_count)
        
    #     i2_error = True
        
    #     for i in range(4):
    #         log_id = str(uuid.uuid4())
    #         quantity = 3
    #         price = 2
            
    #         # Create a user
    #         user_entry = tu.create_user_benchmark()
    #         self.assertTrue(tu.status_code_is_success(user_entry.status_code))
            
    #         # Create an order
    #         order_entry = tu.create_order_benchmark(user_entry.json()['user_id'])
    #         self.assertTrue(tu.status_code_is_success(order_entry.status_code))
            
    #         # Create an item
    #         item_entry = tu.create_item_benchmark(price)
    #         self.assertTrue(tu.status_code_is_success(item_entry.status_code))
            
    #         user_id = user_entry.json()['user_id']
    #         order_id = order_entry.json()['order_id']
    #         item_id = item_entry.json()['item_id']
    #         endpoint_url = f"{tu.ORDER_URL}/orders/add/item/{order_id}/{item_id}/{quantity}"
            
    #         # Create an entry for the receive from user log
    #         if i >= 0:
    #             log1_resp = tu.create_order_log(
    #                 log_id=log_id,
    #                 type=LogType.RECEIVED,
    #                 from_url="BENCHMARK",
    #                 to_url=endpoint_url,
    #                 status=LogStatus.PENDING,
    #             )
    #             self.assertTrue(tu.status_code_is_success(log1_resp.status_code))
                
    #             order_log_count += 1
    #             self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
    #         request_url = f"{tu.STOCK_URL}/stock/find/{item_id}"
            
    #         # Create an entry for the sent to stock log
    #         if i >= 1:
    #             log2_resp = tu.create_order_log(
    #                 log_id=log_id,
    #                 type=LogType.SENT,
    #                 from_url=endpoint_url,
    #                 to_url=request_url,
    #                 status=LogStatus.PENDING,
    #             )
    #             self.assertTrue(tu.status_code_is_success(log2_resp.status_code))
                
    #             order_log_count += 1
    #             self.assertEqual(int(tu.get_order_log_count()), order_log_count)        
            
    #         if i >= 2:
    #             if i2_error: # Create an entry for the received from stock log (failure)
    #                 log3_resp = tu.create_order_log(
    #                     log_id=log_id,
    #                     type=LogType.RECEIVED,
    #                     from_url=request_url,
    #                     to_url=endpoint_url,
    #                     status=LogStatus.FAILURE,
    #                 )
    #                 self.assertTrue(tu.status_code_is_success(log3_resp.status_code))
                    
    #                 order_log_count += 1
    #                 self.assertEqual(int(tu.get_order_log_count()), order_log_count)
                    
    #                 i2_error = False
    #             else: # Create an entry for the received from stock log (success)
    #                 log4_resp = tu.create_order_log(
    #                     log_id=log_id,
    #                     type=LogType.RECEIVED,
    #                     from_url=request_url,
    #                     to_url=endpoint_url,
    #                     status=LogStatus.SUCCESS,
    #                 )
    #                 self.assertTrue(tu.status_code_is_success(log4_resp.status_code))
                    
    #                 order_log_count += 1
    #                 self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
    #         # Create an entry for the update log
    #         if i >= 3:
    #             log5_resp = tu.create_order_log(
    #                 log_id=log_id,
    #                 type=LogType.UPDATE,
    #                 order_id=order_id,
    #                 old_ordervalue=OrderValue(user_id=user_id, paid=False, items=[], total_cost=0),
    #                 new_ordervalue=OrderValue(user_id=user_id, paid=False, items=[(item_id, int(quantity))], total_cost=price*quantity),
    #             )
    #             self.assertTrue(tu.status_code_is_success(log5_resp.status_code))
                
    #             order_log_count += 1
    #             self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
    #         ft_resp = tu.fault_tolerance_order()
    #         self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
            
    #         order_log_count -= i+1
    #         self.assertEqual(tu.get_order_log_count(), order_log_count)
        
        
    # def test_checkout_contains_faulty_log(self):
    #     # Get initial log count
    #     order_log_count = int(tu.get_order_log_count())
        
    #     i2_error = i4_error = i42_error = True
        
    #     i = 0
    #     while i < 5:
    #         log_id = str(uuid.uuid4())
    #         quantity = 3
    #         price = 2
    #         credit = 100
            
    #         # Create a user
    #         user_entry = tu.create_user_benchmark()
    #         self.assertTrue(tu.status_code_is_success(user_entry.status_code))
    #         user_id = user_entry.json()['user_id']
            
    #         # Add credit to user
    #         credit_added = tu.add_credit_to_user_benchmark(user_id, credit)
    #         self.assertTrue(tu.status_code_is_success(credit_added.status_code))
            
    #         # Verify credit was added
    #         find_user = tu.find_user_benchmark(user_id)
    #         self.assertTrue(tu.status_code_is_success(find_user.status_code))
    #         self.assertEqual(find_user.json()['credit'], credit)
            
    #         # Create an order
    #         order_entry = tu.create_order_benchmark(user_id)
    #         self.assertTrue(tu.status_code_is_success(order_entry.status_code))
    #         order_id = order_entry.json()['order_id']
            
    #         # Create an item
    #         item_entry = tu.create_item_benchmark(price)
    #         self.assertTrue(tu.status_code_is_success(item_entry.status_code))
    #         item_id = item_entry.json()['item_id']
            
    #         # Add stock to item
    #         stock_added = tu.add_stock_benchmark(item_id, quantity)
    #         self.assertTrue(tu.status_code_is_success(stock_added.status_code))
            
    #         # Verify stock was added
    #         find_item = tu.find_item_benchmark(item_id)
    #         self.assertTrue(tu.status_code_is_success(find_item.status_code))
    #         self.assertEqual(find_item.json()['stock'], quantity)
            
    #         # Add item to order
    #         item_added = tu.add_item_to_order_benchmark(order_id, item_id, quantity)
    #         self.assertTrue(tu.status_code_is_success(item_added.status_code))
            
    #         # Verify item was added to order
    #         find_order = tu.find_order_benchmark(order_id)
    #         self.assertTrue(tu.status_code_is_success(find_order.status_code))
    #         self.assertEqual(find_order.json()['items'][0][0], item_id)
            
    #         endpoint_url = f"{tu.ORDER_URL}/orders/checkout/{order_id}"
            
    #         # Create an entry for the received from user log
    #         if i >= 0:
    #             log1_resp = tu.create_order_log(
    #                 log_id=log_id,
    #                 type=LogType.RECEIVED,
    #                 from_url="BENCHMARK",
    #                 to_url=endpoint_url,
    #                 status=LogStatus.PENDING,
    #             )
    #             self.assertTrue(tu.status_code_is_success(log1_resp.status_code))
                
    #             order_log_count += 1
    #             self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
    #         stock_request_url = f"{tu.STOCK_URL}/stock/subtract/{item_id}/{quantity}"
            
    #         # Create an entry for the sent to stock log
    #         if i >= 1:
    #             log2_resp = tu.create_order_log(
    #                 log_id=log_id,
    #                 type=LogType.SENT,
    #                 from_url=endpoint_url,
    #                 to_url=stock_request_url,
    #                 status=LogStatus.PENDING,
    #             )
    #             self.assertTrue(tu.status_code_is_success(log2_resp.status_code))
                
    #             order_log_count += 1
    #             self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
    #         # Create an entry for the received from stock log
    #         if i >= 2:
    #             if i2_error:
    #                 log3_resp = tu.create_order_log(
    #                     log_id=log_id,
    #                     type=LogType.RECEIVED,
    #                     from_url=stock_request_url,
    #                     to_url=endpoint_url,
    #                     status=LogStatus.FAILURE,
    #                 )
    #                 self.assertTrue(tu.status_code_is_success(log3_resp.status_code))
                    
    #                 order_log_count += 1
    #                 self.assertEqual(int(tu.get_order_log_count()), order_log_count)
    #             else:
    #                 log4_resp = tu.create_order_log(
    #                     log_id=log_id,
    #                     type=LogType.RECEIVED,
    #                     from_url=stock_request_url,
    #                     to_url=endpoint_url,
    #                     status=LogStatus.SUCCESS,
    #                 )
    #                 self.assertTrue(tu.status_code_is_success(log4_resp.status_code))
                    
    #                 order_log_count += 1
    #                 self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
    #         rollback_stock_request_url = f"{tu.STOCK_URL}/stock/add/{item_id}/{quantity}"
            
    #         # Create an entry for the sent rollback to stock log
    #         if i >= 3 and i2_error:
    #             log5_resp = tu.create_order_log(
    #                 log_id=log_id,
    #                 type=LogType.SENT,
    #                 from_url=endpoint_url,
    #                 to_url=rollback_stock_request_url,
    #                 status=LogStatus.PENDING,
    #             )
    #             self.assertTrue(tu.status_code_is_success(log5_resp.status_code))
                
    #             order_log_count += 1
    #             self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
    #         # Create an entry for the received rollback from stock log
    #         if i >= 4 and i2_error:
    #             if i4_error:
    #                 log6_resp = tu.create_order_log(
    #                     log_id=log_id,
    #                     type=LogType.RECEIVED,
    #                     from_url=rollback_stock_request_url,
    #                     to_url=endpoint_url,
    #                     status=LogStatus.FAILURE,
    #                 )
    #                 self.assertTrue(tu.status_code_is_success(log6_resp.status_code))
                    
    #                 order_log_count += 1
    #                 self.assertEqual(int(tu.get_order_log_count()), order_log_count)
    #             else:
    #                 log7_resp = tu.create_order_log(
    #                     log_id=log_id,
    #                     type=LogType.RECEIVED,
    #                     from_url=rollback_stock_request_url,
    #                     to_url=endpoint_url,
    #                     status=LogStatus.SUCCESS,
    #                 )
    #                 self.assertTrue(tu.status_code_is_success(log7_resp.status_code))
                    
    #                 order_log_count += 1
    #                 self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
    #         payment_request_url = f"{tu.PAYMENT_URL}/payment/pay/{user_id}/{price*quantity}"
            
    #         # Create an entry for the sent to payment log
    #         if i >= 3 and not i2_error:
    #             log8_resp = tu.create_order_log(
    #                 log_id=log_id,
    #                 type=LogType.SENT,
    #                 from_url=endpoint_url,
    #                 to_url=payment_request_url,
    #                 status=LogStatus.PENDING,
    #             )
    #             self.assertTrue(tu.status_code_is_success(log8_resp.status_code))
                
    #             order_log_count += 1
    #             self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
    #         # Create an entry for the received from payment log
    #         if i >= 4 and not i2_error:
    #             if i42_error:
    #                 log9_resp = tu.create_order_log(
    #                     log_id=log_id,
    #                     type=LogType.RECEIVED,
    #                     from_url=payment_request_url,
    #                     to_url=endpoint_url,
    #                     status=LogStatus.FAILURE,
    #                 )
    #                 self.assertTrue(tu.status_code_is_success(log9_resp.status_code))
                    
    #                 order_log_count += 1
    #                 self.assertEqual(int(tu.get_order_log_count()), order_log_count)
                    
    #                 i42_error = False
    #             else:
    #                 log10_resp = tu.create_order_log(
    #                 log_id=log_id,
    #                 type=LogType.RECEIVED,
    #                 from_url=payment_request_url,
    #                 to_url=endpoint_url,
    #                 status=LogStatus.SUCCESS,
    #             )
    #                 self.assertTrue(tu.status_code_is_success(log10_resp.status_code))
                    
    #                 order_log_count += 1
    #                 self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
    #         # Create an entry for the update log
    #         if i >= 5:
    #             log11_resp = tu.create_order_log(
    #                 log_id=log_id,
    #                 type=LogType.UPDATE,
    #                 order_id=order_id,
    #                 old_ordervalue=OrderValue(user_id=user_id, paid=False, items=[(item_id, int(quantity))], total_cost=price*quantity),
    #                 new_ordervalue=OrderValue(user_id=user_id, paid=True, items=[(item_id, int(quantity))], total_cost=price*quantity),
    #             )
    #             self.assertTrue(tu.status_code_is_success(log11_resp.status_code))
                
    #             order_log_count += 1
    #             self.assertEqual(int(tu.get_order_log_count()), order_log_count)
            
    #         ft_resp = tu.fault_tolerance_order()
    #         self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
            
    #         order_log_count -= i+1
    #         self.assertEqual(tu.get_order_log_count(), order_log_count)
            
    #         if i >= 4 and i2_error and i4_error:
    #             i4_error = False
    #             i -= 1
    #         elif i >= 4 and i2_error:
    #             i2_error = False
    #             i -= 2
            
    #         i += 1
        
        
    # def test_checkout_stock_rollback_contains_faulty_log(self):
    #     log_count = int(tu.get_order_log_count())
        
    #     log_id = str(uuid.uuid4())
    #     checkout_url = f"{tu.ORDER_URL}/orders/checkout/1"
        
    #     # Create item 1
    #     item1 = tu.create_item_benchmark(2)
    #     self.assertTrue(tu.status_code_is_success(item1.status_code))
    #     item1_id = item1.json()['item_id']
        
    #     # Add stock to item 1
    #     stock_added = tu.add_stock_benchmark(item1_id, 20)
    #     self.assertTrue(tu.status_code_is_success(stock_added.status_code))
    #     item1_stock = tu.find_item_benchmark(item1_id).json()['stock']
        
    #     # Create log for receive from user
    #     log1_resp = tu.create_order_log(
    #         log_id=log_id,
    #         type=LogType.RECEIVED,
    #         from_url="BENCHMARK",
    #         to_url=checkout_url,
    #         status=LogStatus.PENDING,
    #     )
    #     self.assertTrue(tu.status_code_is_success(log1_resp.status_code))
        
    #     log_count += 1
    #     self.assertEqual(int(tu.get_order_log_count()), log_count)
        
    #     item1_subtract_url = f"{tu.STOCK_URL}/stock/subtract/{item1_id}/1"
        
    #     # Create log for send to stock (item 1)
    #     log2_resp = tu.create_order_log(
    #         log_id=log_id,
    #         type=LogType.SENT,
    #         from_url=checkout_url,
    #         to_url=item1_subtract_url,
    #         status=LogStatus.PENDING,
    #     )
    #     self.assertTrue(tu.status_code_is_success(log2_resp.status_code))
        
    #     log_count += 1
    #     self.assertEqual(int(tu.get_order_log_count()), log_count)
        
    #     # Create log for receive from stock (success)
    #     log3_resp = tu.create_order_log(
    #         log_id=log_id,
    #         type=LogType.RECEIVED,
    #         from_url=item1_subtract_url,
    #         to_url=checkout_url,
    #         status=LogStatus.SUCCESS,
    #     )
    #     self.assertTrue(tu.status_code_is_success(log3_resp.status_code))   
        
    #     log_count += 1
    #     self.assertEqual(int(tu.get_order_log_count()), log_count) 
        
    #     # Create item 2
    #     item2 = tu.create_item_benchmark(2)
    #     self.assertTrue(tu.status_code_is_success(item2.status_code))
    #     item2_id = item2.json()['item_id']
        
    #     # Add stock to item 2
    #     stock_added = tu.add_stock_benchmark(item2_id, 20)
    #     item2_stock = tu.find_item_benchmark(item2_id).json()['stock']
        
    #     item2_subtract_url = f"{tu.STOCK_URL}/stock/subtract/{item2_id}/1"
        
    #     # Create log for send to stock (item 2)
    #     log4_resp = tu.create_order_log(
    #         log_id=log_id,
    #         type=LogType.SENT,
    #         from_url=checkout_url,
    #         to_url=item2_subtract_url,
    #         status=LogStatus.PENDING,
    #     )
    #     self.assertTrue(tu.status_code_is_success(log4_resp.status_code))
        
    #     log_count += 1
    #     self.assertEqual(int(tu.get_order_log_count()), log_count)
        
    #     # Create log for receive from stock (success)
    #     log5_resp = tu.create_order_log(
    #         log_id=log_id,
    #         type=LogType.RECEIVED,
    #         from_url=item2_subtract_url,
    #         to_url=checkout_url,
    #         status=LogStatus.SUCCESS,
    #     )
    #     self.assertTrue(tu.status_code_is_success(log5_resp.status_code))
        
    #     log_count += 1
    #     self.assertEqual(int(tu.get_order_log_count()), log_count)
        
    #     # Create item 3
    #     item3 = tu.create_item_benchmark(2)
    #     self.assertTrue(tu.status_code_is_success(item3.status_code))
    #     item3_id = item3.json()['item_id']
        
    #     item3_subtract_url = f"{tu.STOCK_URL}/stock/subtract/{item3_id}/1"
        
    #     # Create log for send to stock (item 3)
    #     log6_resp = tu.create_order_log(
    #         log_id=log_id,
    #         type=LogType.SENT,
    #         from_url=checkout_url,
    #         to_url=item3_subtract_url,
    #         status=LogStatus.PENDING,
    #     )
    #     self.assertTrue(tu.status_code_is_success(log6_resp.status_code))
        
    #     log_count += 1
    #     self.assertEqual(int(tu.get_order_log_count()), log_count)
        
    #     # Create log for receive from stock (failure)
    #     log7_resp = tu.create_order_log(
    #         log_id=log_id,
    #         type=LogType.RECEIVED,
    #         from_url=item3_subtract_url,
    #         to_url=checkout_url,
    #         status=LogStatus.FAILURE,
    #     )
    #     self.assertTrue(tu.status_code_is_success(log7_resp.status_code))
        
    #     log_count += 1
    #     self.assertEqual(int(tu.get_order_log_count()), log_count)
        
    #     item1_add_url = f"{tu.STOCK_URL}/stock/add/{item1_id}/1"
    #     item2_add_url = f"{tu.STOCK_URL}/stock/add/{item2_id}/1"
        
    #     # Create log for send rollback to stock (item 1)
    #     log8_resp = tu.create_order_log(
    #         log_id=log_id,
    #         type=LogType.SENT,
    #         from_url=checkout_url,
    #         to_url=item1_add_url,
    #         status=LogStatus.PENDING,
    #     )
    #     self.assertTrue(tu.status_code_is_success(log8_resp.status_code))
        
    #     log_count += 1
    #     self.assertEqual(int(tu.get_order_log_count()), log_count)
        
    #     # Create log for receive rollback from stock (success)
    #     log9_resp = tu.create_order_log(
    #         log_id=log_id,
    #         type=LogType.RECEIVED,
    #         from_url=item1_add_url,
    #         to_url=checkout_url,
    #         status=LogStatus.SUCCESS,
    #     )
    #     self.assertTrue(tu.status_code_is_success(log9_resp.status_code))
        
    #     log_count += 1
    #     self.assertEqual(int(tu.get_order_log_count()), log_count)
        
    #     # Create log for send rollback to stock (item 2)
    #     log10_resp = tu.create_order_log(
    #         log_id=log_id,
    #         type=LogType.SENT,
    #         from_url=checkout_url,
    #         to_url=item2_add_url,
    #         status=LogStatus.PENDING,
    #     )
    #     self.assertTrue(tu.status_code_is_success(log10_resp.status_code))
        
    #     log_count += 1
    #     self.assertEqual(int(tu.get_order_log_count()), log_count)
        
    #     # Create log for receive rollback from stock (failure)
    #     log11_resp = tu.create_order_log(
    #         log_id=log_id,
    #         type=LogType.RECEIVED,
    #         from_url=item2_add_url,
    #         to_url=checkout_url,
    #         status=LogStatus.FAILURE,
    #     )
    #     self.assertTrue(tu.status_code_is_success(log11_resp.status_code))
        
    #     log_count += 1
    #     self.assertEqual(int(tu.get_order_log_count()), log_count)
        
    #     ft_resp = tu.fault_tolerance_order()
    #     self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
        
    #     log_count -= 11
    #     self.assertEqual(int(tu.get_order_log_count()), log_count)
        
    #     # Check if stock was rolled back
    #     find_item1 = tu.find_item_benchmark(item1_id)
    #     self.assertTrue(tu.status_code_is_success(find_item1.status_code))
        
    #     find_item1_stock = find_item1.json()['stock']
    #     self.assertEqual(find_item1_stock, item1_stock)
        
    #     find_item2 = tu.find_item_benchmark(item2_id)
    #     self.assertTrue(tu.status_code_is_success(find_item2.status_code))
        
    #     find_item2_stock = find_item2.json()['stock']
    #     self.assertEqual(find_item2_stock, item2_stock+1)
        
        
if __name__ == '__main__':
    unittest.main()