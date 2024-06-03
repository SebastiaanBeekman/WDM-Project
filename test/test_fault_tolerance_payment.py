import unittest

import uuid
import utils as tu
from class_utils import LogType, LogStatus, UserValue


class TestMicroservices(unittest.TestCase):

    def test_payment_contains_no_faulty_logs(self):
        # Get initial log count
        payment_log_count = int(tu.get_payment_log_count())

        # Test /user/create
        user1: dict = tu.create_user()
        self.assertIn('user_id', user1)
        self.assertIn('log_id', user1)

        # Check if log count increased by 2
        payment_log_count += 2
        self.assertEqual(int(tu.get_payment_log_count()), payment_log_count)

        # Check if last log is correct
        payment_log = tu.get_payment_log()
        last_create_log = payment_log[user1['log_id']][-1]["log"]
        self.assertEqual(last_create_log['type'], "Sent")
        self.assertEqual(last_create_log["status"], "Success")

        # Test /user/find
        user2 = tu.find_user(user1['user_id'])
        self.assertIn('credit', user2)

        # Check if log count not increased
        self.assertEqual(int(tu.get_payment_log_count()), payment_log_count)

        # Test /add_funds
        add_credit_response = tu.add_credit_to_user(user1['user_id'], 15)
        self.assertTrue(tu.status_code_is_success(add_credit_response))

        # Check if log count increased by 3
        payment_log_count += 2
        self.assertEqual(int(tu.get_payment_log_count()), payment_log_count)

        # Check if last log is correct
        payment_log = tu.get_payment_log()
        last_add_log = payment_log[user1['log_id']][-1]["log"]
        self.assertEqual(last_add_log['type'], "Sent")
        self.assertEqual(last_add_log["status"], "Success")

        # Test /pay/
        pay_response = tu.payment_pay(user1['user_id'], 15)
        self.assertTrue(tu.status_code_is_success(pay_response))

        # Check if log count increased by 3
        payment_log_count += 2
        self.assertEqual(int(tu.get_payment_log_count()), payment_log_count)

        # Check if last log is correct
        payment_log = tu.get_payment_log()
        last_pay_log = payment_log[user1['log_id']][-1]["log"]
        self.assertEqual(last_pay_log['type'], "Sent")
        self.assertEqual(last_pay_log["status"], "Success")


    def test_payment_create_contains_faulty_log(self):
        # Get initial log count
        payment_log_count = int(tu.get_payment_log_count())
        self.assertIsNotNone(payment_log_count)

        log_id = str(uuid.uuid4())
        credit = 5

        # Create a user
        user1_resp = tu.create_user_benchmark()
        self.assertTrue(tu.status_code_is_success(user1_resp.status_code))
        
        add_credit_resp = tu.add_credit_to_user_benchmark(user1_resp.json()['user_id'], credit)
        self.assertTrue(tu.status_code_is_success(add_credit_resp.status_code))

        user1_id = user1_resp.json()['user_id']

        find_user1_resp = tu.find_user_benchmark(user1_id)
        self.assertTrue(tu.status_code_is_success(find_user1_resp.status_code))
        self.assertEqual(find_user1_resp.json()['credit'], credit)

        # Create an entry for the create log
        log1_resp = tu.create_payment_log(
            log_id=log_id,
            type=LogType.CREATE,
            user_id=user1_id,
        )
        self.assertTrue(tu.status_code_is_success(log1_resp.status_code))

        payment_log_count += 1
        self.assertEqual(int(tu.get_payment_log_count()), payment_log_count)

        ft_resp = tu.fault_tolerance_payment()
        self.assertTrue(tu.status_code_is_success(ft_resp.status_code))

        payment_log_count -= 1
        self.assertEqual(tu.get_payment_log_count(), payment_log_count)

        # Check whether user was deleted
        find_user1_resp = tu.find_user_benchmark(user1_id)
        self.assertTrue(tu.status_code_is_failure(find_user1_resp.status_code))


    def test_add_funds_contains_faulty_log(self):
        # Get initial log count
        payment_log_count = int(tu.get_payment_log_count())
        credit = 5
        self.assertIsNotNone(payment_log_count)

        log_id = str(uuid.uuid4())

        user_entry = tu.create_user_benchmark()
        self.assertTrue(tu.status_code_is_success(user_entry.status_code))

        user_id = user_entry.json()['user_id']

        # Update the stock of the item
        add_funds_resp = tu.add_credit_to_user_benchmark(user_id, credit)
        self.assertTrue(tu.status_code_is_success(add_funds_resp.status_code))

        find_user_resp = tu.find_user_benchmark(user_id)
        self.assertTrue(tu.status_code_is_success(find_user_resp.status_code))
        self.assertEqual(find_user_resp.json()['credit'], credit)

        # Create an entry for the update log
        log2_resp = tu.create_payment_log(
            log_id=log_id,
            type=LogType.UPDATE,
            user_id=user_id,
            old_uservalue=UserValue(credit=credit),
        )
        self.assertTrue(tu.status_code_is_success(log2_resp.status_code))

        payment_log_count += 1
        self.assertEqual(int(tu.get_payment_log_count()), payment_log_count)

        ft_resp = tu.fault_tolerance_payment()
        self.assertTrue(tu.status_code_is_success(ft_resp.status_code))

        payment_log_count -= 1
        self.assertEqual(tu.get_payment_log_count(), payment_log_count)


    def test_pay_contains_faulty_log(self):
        # Get initial log count
        payment_log_count = int(tu.get_payment_log_count())
        self.assertIsNotNone(payment_log_count)

        log_id = str(uuid.uuid4())
        credit = 5

        user_entry = tu.create_user_benchmark()

        self.assertTrue(tu.status_code_is_success(user_entry.status_code))

        user_id = user_entry.json()['user_id']

        add_funds_resp = tu.add_credit_to_user_benchmark(user_id, credit)
        self.assertTrue(tu.status_code_is_success(add_funds_resp.status_code))

        # Update the stock of the item
        pay_resp = tu.payment_pay_benchmark(user_id, credit)
        self.assertTrue(tu.status_code_is_success(pay_resp.status_code))

        find_user_resp = tu.find_user_benchmark(user_id)
        self.assertTrue(tu.status_code_is_success(find_user_resp.status_code))
        self.assertEqual(find_user_resp.json()['credit'], 0)

        # Create an entry for the update log
        log2_resp = tu.create_payment_log(
            log_id=log_id,
            type=LogType.UPDATE,
            user_id=user_id,
            old_uservalue=UserValue(credit=credit),
        )
        self.assertTrue(tu.status_code_is_success(log2_resp.status_code))

        payment_log_count += 1
        self.assertEqual(int(tu.get_payment_log_count()), payment_log_count)

        ft_resp = tu.fault_tolerance_payment()
        self.assertTrue(tu.status_code_is_success(ft_resp.status_code))

        payment_log_count -= 1
        self.assertEqual(tu.get_payment_log_count(), payment_log_count)


if __name__ == '__main__':
    unittest.main()
