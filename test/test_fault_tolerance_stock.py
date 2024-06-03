import unittest

import uuid
import utils as tu
from class_utils import LogType, LogStatus, StockValue

class TestMicroservices(unittest.TestCase):

    def test_stock_contains_no_faulty_logs(self):
        # Get initial log count
        stock_log_count = int(tu.get_stock_log_count())
        
        # Test /stock/create
        item1: dict = tu.create_item(5)
        self.assertIn('item_id', item1)
        self.assertIn('log_id', item1)
        
        # Check if log count increased by 2
        stock_log_count += 2
        self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
        
        # Check if last log is correct
        stock_log = tu.get_stock_log()
        last_create_log = stock_log[item1['log_id']][-1]["log"]
        self.assertEqual(last_create_log['type'], "Sent")
        self.assertEqual(last_create_log["status"], "Success")
        
        # Test /stock/find
        item2 = tu.find_item(item1['item_id'])
        self.assertIn('price', item2)
        
        # Check if log count not increased
        self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
        
        # Test /stock/add
        add_stock_response = tu.add_stock(item1['item_id'], 15)
        self.assertTrue(tu.status_code_is_success(add_stock_response))
        
        # Check if log count increased by 2
        stock_log_count += 2
        self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
        
        # Check if last log is correct
        stock_log = tu.get_stock_log()
        last_add_log = stock_log[item1['log_id']][-1]["log"]
        self.assertEqual(last_add_log['type'], "Sent")
        self.assertEqual(last_add_log["status"], "Success")
        
        # Test /stock/subtract
        subtract_stock_response = tu.subtract_stock(item1['item_id'], 15)
        self.assertTrue(tu.status_code_is_success(subtract_stock_response))
        
        # Check if log count increased by 2
        stock_log_count += 2
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
        
        log_id = str(uuid.uuid4())
        price = 5

        # Create an item with the given price
        item1_resp = tu.create_item_benchmark(price)
        self.assertTrue(tu.status_code_is_success(item1_resp.status_code))
        
        item1_id = item1_resp.json()['item_id']
        
        find_item1_resp = tu.find_item_benchmark(item1_id)
        self.assertTrue(tu.status_code_is_success(find_item1_resp.status_code))
        self.assertEqual(find_item1_resp.json()['price'], price)
    
        # Create an entry for the create log
        log3_resp = tu.create_stock_log(
            log_id=log_id,
            type=LogType.CREATE,
            stock_id=item1_id,
        )
        self.assertTrue(tu.status_code_is_success(log3_resp.status_code))
        
        stock_log_count += 1
        self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
    
        ft_resp = tu.fault_tolerance_stock()
        self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
        
        stock_log_count -= 1
        self.assertEqual(tu.get_stock_log_count(), stock_log_count)
        
        # Check whether item was deleted
        find_item1_resp = tu.find_item_benchmark(item1_id)
        self.assertTrue(tu.status_code_is_failure(find_item1_resp.status_code))


    def test_stock_add_contains_faulty_log(self):
        # Get initial log count
        stock_log_count = int(tu.get_stock_log_count())
        self.assertIsNotNone(stock_log_count)
        
        log_id = str(uuid.uuid4())
        amount = 5
        
        item_entry = tu.create_item_benchmark(amount)
        self.assertTrue(tu.status_code_is_success(item_entry.status_code))
        
        item_id = item_entry.json()['item_id']
        
        # Update the stock of the item
        add_stock_resp = tu.add_stock_benchmark(item_id, amount)
        self.assertTrue(tu.status_code_is_success(add_stock_resp.status_code))
        
        find_item_resp = tu.find_item_benchmark(item_id)
        self.assertTrue(tu.status_code_is_success(find_item_resp.status_code))
        self.assertEqual(find_item_resp.json()['stock'], amount)
            
        # Create an entry for the update log
        log2_resp = tu.create_stock_log(
            log_id=log_id,
            type=LogType.UPDATE,
            stock_id=item_id,
            old_stockvalue=StockValue(stock=0, price=amount),
        )
        self.assertTrue(tu.status_code_is_success(log2_resp.status_code))
        
        stock_log_count += 1
        self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
    
        ft_resp = tu.fault_tolerance_stock()
        self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
        
        stock_log_count -= 1
        self.assertEqual(tu.get_stock_log_count(), stock_log_count)
    
            
            
    def test_stock_subtract_contains_faulty_log(self):
        # Get initial log count
        stock_log_count = int(tu.get_stock_log_count())
        self.assertIsNotNone(stock_log_count)
        
        log_id = str(uuid.uuid4())
        amount = 5
        
        item_entry = tu.create_item_benchmark(amount)
        self.assertTrue(tu.status_code_is_success(item_entry.status_code))
        
        item_id = item_entry.json()['item_id']
        
        add_stock_resp = tu.add_stock_benchmark(item_id, amount)
        self.assertTrue(tu.status_code_is_success(add_stock_resp.status_code))
        
        # Update the stock of the item
        add_stock_resp = tu.subtract_stock_benchmark(item_id, amount)
        self.assertTrue(tu.status_code_is_success(add_stock_resp.status_code))
        
        find_item_resp = tu.find_item_benchmark(item_id)
        self.assertTrue(tu.status_code_is_success(find_item_resp.status_code))
        self.assertEqual(find_item_resp.json()['stock'], 0)
            
        # Create an entry for the update log
        log2_resp = tu.create_stock_log(
            log_id=log_id,
            type=LogType.UPDATE,
            stock_id=item_id,
            old_stockvalue=StockValue(stock=amount, price=amount),
        )
        self.assertTrue(tu.status_code_is_success(log2_resp.status_code))
        
        stock_log_count += 1
        self.assertEqual(int(tu.get_stock_log_count()), stock_log_count)
        
        ft_resp = tu.fault_tolerance_stock()
        self.assertTrue(tu.status_code_is_success(ft_resp.status_code))
        
        stock_log_count -= 1
        self.assertEqual(tu.get_stock_log_count(), stock_log_count)

if __name__ == '__main__':
    unittest.main()
