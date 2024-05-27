import unittest

import uuid
import utils as tu

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
            tu.create_received_from_user_log(log_id)

            item_id = str(uuid.uuid4())

            # Create a log entry for the create request
            create_payload = LogStockValue(
                id=log_id,
                type=LogType.CREATE,
                stock_id=item_id,
                new_stockvalue=stock_value,
                dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
            )

            # Set the log entry and the updated item in the pipeline
            log_key = get_key()
            pipeline_db.set(log_key, msgpack.encode(create_payload))
            pipeline_db.set(item_id, msgpack.encode(stock_value))
            try:
                pipeline_db.execute()
            except redis.exceptions.RedisError:
                error_payload = LogStockValue(
                    id=log_id,
                    type=LogType.SENT,
                    from_url=request.url,       # This endpoint
                    to_url=request.referrer,    # Endpoint that called this
                    stock_id=item_id,
                    status=LogStatus.FAILURE,
                    dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
                )
                db.set(get_key(), msgpack.encode(error_payload))
                
                pipeline_db.discard()
                
                return abort(400, DB_ERROR_STR)
            
            # Fault Tollerance: CRASH - Undo

            # Create a log entry for the sent response back to the user
            sent_payload_to_user = LogStockValue(
                id=log_id,
                type=LogType.SENT,
                from_url=request.url,       # This endpoint
                to_url=request.referrer,    # Endpoint that called this
                stock_id=item_id,
                status=LogStatus.SUCCESS,
                dateTime=datetime.now().strftime("%Y%m%d%H%M%S%f")
            )
            db.set(get_key(), msgpack.encode(sent_payload_to_user))


if __name__ == '__main__':
    unittest.main()
