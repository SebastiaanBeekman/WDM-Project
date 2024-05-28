import unittest

import uuid
import utils as tu
from class_utils import LogStockValue, LogType, LogStatus, StockValue


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
        self.assertIsNotNone(stock_log_count)
        
        for i in range(2):
            log_id = str(uuid.uuid4())
            price = 5
            endpoint_url = f"{tu.STOCK_URL}/stock/item/create/{price}"

            # Create an entry for the receive from user log
            if i >= 0:
                log1_resp = tu.create_stock_log(
                    log_id=log_id,
                    type=LogType.RECEIVED,
                    from_url="BENCHMARK",
                    to_url=endpoint_url,
                    status=LogStatus.PENDING,
                )
                self.assertTrue(tu.status_code_is_success(log1_resp.status_code))
                
                stock_log_count += 1
                self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
            
            # Create an item with the given price
            if i >= 1:
                item1_resp = tu.create_item_benchmark(price)
                self.assertTrue(tu.status_code_is_success(item1_resp.status_code))
                
                item1_id = item1_resp.json()['item_id']
                stock_value = StockValue(stock=0, price=int(price))
                
                find_item1_resp = tu.find_item_benchmark(item1_id)
                self.assertTrue(tu.status_code_is_success(find_item1_resp.status_code))
                self.assertEqual(find_item1_resp.json()['price'], price)
            
            # Create an entry for the error log (Unused as the log is finished properly when this is present)
            # if i == 1:
            #     log2_resp = tu.create_stock_log(
            #         log_id=log_id,
            #         type=LogType.SENT,
            #         from_url=endpoint_url,
            #         to_url="BENCHMARK",
            #         stock_id=item1_id,
            #         status=LogStatus.FAILURE,
            #     )
            #     self.assertTrue(tu.status_code_is_success(log2_resp.status_code))
                
            #     stock_log_count += 1
            #     self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
            
            # Create an entry for the create log
            if i >= 1:
                log3_resp = tu.create_stock_log(
                    log_id=log_id,
                    type=LogType.CREATE,
                    stock_id=item1_id,
                    new_stockvalue=stock_value,
                )
                self.assertTrue(tu.status_code_is_success(log3_resp.status_code))
                
                stock_log_count += 1
                self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
            
            # Create an entry for the sent to user log (Unused as the log is finished properly when this is present)
            # if i >= 3:
            #     log4_resp = tu.create_stock_log(
            #         log_id=log_id,
            #         type=LogType.SENT,
            #         from_url=endpoint_url,
            #         to_url="BENCHMARK",
            #         stock_id=item1_id,
            #         status=LogStatus.SUCCESS,
            #         old_stockvalue=stock_value,
            #     )
            #     self.assertTrue(tu.status_code_is_success(log4_resp.status_code))
                
            #     stock_log_count += 1
            #     self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
            
            ft_resp = tu.fault_tolerance_stock()
            self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
            
            stock_log_count -= i+1
            self.assertEqual(tu.get_stock_log_count(), stock_log_count)
            
            # Check whether item was deleted
            if i >= 1:
                find_item1_resp = tu.find_item_benchmark(item1_id)
                self.assertTrue(tu.status_code_is_failure(find_item1_resp.status_code))


if __name__ == '__main__':
    unittest.main()
