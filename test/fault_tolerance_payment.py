import unittest

import uuid
import utils
from class_utils import LogType, LogStatus, UserValue


class TestMicroservices(unittest.TestCase):

    def test_payment_contains_no_faulty_logs(self):
        # Get initial log count
        payment_log_count = int(utils.get_payment_log_count())

        # Test /user/create
        user1: dict = utils.create_user()
        self.assertIn('user_id', user1)
        self.assertIn('log_id', user1)

        # Check if log count increased by 3
        payment_log_count += 3
        self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

        # Check if last log is correct
        payment_log = utils.get_payment_log()
        last_create_log = payment_log[user1['log_id']][-1]["log"]
        self.assertEqual(last_create_log['type'], "Sent")
        self.assertEqual(last_create_log["status"], "Success")

        # Test /user/find
        user2 = utils.find_user(user1['user_id'])
        self.assertIn('credit', user2)

        # Check if log count increased by 2
        payment_log_count += 2
        self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

        # Check if last log is correct
        payment_log = utils.get_payment_log()
        last_find_log = payment_log[user2['log_id']][-1]["log"]
        self.assertEqual(last_find_log['type'], "Sent")
        self.assertEqual(last_find_log["status"], "Success")

        # Test /add_funds
        add_credit_response = utils.add_credit_to_user(user1['user_id'], 15)
        self.assertTrue(utils.status_code_is_success(add_credit_response))

        # Check if log count increased by 3
        payment_log_count += 3
        self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

        # Check if last log is correct
        payment_log = utils.get_payment_log()
        last_add_log = payment_log[user1['log_id']][-1]["log"]
        self.assertEqual(last_add_log['type'], "Sent")
        self.assertEqual(last_add_log["status"], "Success")

        # Test /pay/
        pay_response = utils.payment_pay(user1['user_id'], 15)
        self.assertTrue(utils.status_code_is_success(pay_response))

        # Check if log count increased by 3
        payment_log_count += 3
        self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

        # Check if last log is correct
        payment_log = utils.get_payment_log()
        last_pay_log = payment_log[user1['log_id']][-1]["log"]
        self.assertEqual(last_pay_log['type'], "Sent")
        self.assertEqual(last_pay_log["status"], "Success")

    def test_payment_create_contains_faulty_log(self):
        # Get initial log count
        payment_log_count = int(utils.get_payment_log_count())
        self.assertIsNotNone(payment_log_count)

        for i in range(2):
            log_id = str(uuid.uuid4())
            endpoint_url = f"{utils.PAYMENT_URL}/payment/create_user"

            # Create an entry for the receive from user log
            if i >= 0:
                log1_resp = utils.create_payment_log(
                    log_id=log_id,
                    type=LogType.RECEIVED,
                    from_url="BENCHMARK",
                    to_url=endpoint_url,
                    status=LogStatus.PENDING,
                )
                self.assertTrue(utils.status_code_is_success(log1_resp.status_code))

                payment_log_count += 1
                self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

            # Create a user
            if i >= 1:
                user1_resp = utils.create_user_benchmark()
                self.assertTrue(utils.status_code_is_success(user1_resp.status_code))

                user1_id = user1_resp.json()['user_id']

                find_user1_resp = utils.find_user_benchmark(user1_id)
                self.assertTrue(utils.status_code_is_success(find_user1_resp.status_code))

            # Create an entry for the error log (Unused as the log is finished properly when this is present)
            # if i == 1:
            #     log2_resp = utils.create_payment_log(
            #         log_id=log_id,
            #         type=LogType.SENT,
            #         from_url=endpoint_url,
            #         to_url="BENCHMARK",
            #         user_id=user1_id,
            #         status=LogStatus.FAILURE,
            #     )
            #     self.assertTrue(utils.status_code_is_success(log2_resp.status_code))

            #     stock_payment_count += 1
            #     self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

            # Create an entry for the create log
            if i >= 1:
                user_value = UserValue(10)
                log3_resp = utils.create_payment_log(
                    log_id=log_id,
                    type=LogType.CREATE,
                    user_id=user1_id,
                    new_uservalue=user_value,
                )
                self.assertTrue(utils.status_code_is_success(log3_resp.status_code))

                payment_log_count += 1
                self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

            # Create an entry for the sent to user log (Unused as the log is finished properly when this is present)
            # if i >= 3:
            #     log4_resp = utils.create_payment_log(
            #         log_id=log_id,
            #         type=LogType.SENT,
            #         from_url=endpoint_url,
            #         to_url="BENCHMARK",
            #         user_id=user1_id,
            #         status=LogStatus.SUCCESS,
            #         old_uservalue=user_value,
            #     )
            #     self.assertTrue(utils.status_code_is_success(log4_resp.status_code))

            #     user_log_count += 1
            #     self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

            ft_resp = utils.fault_tolerance_payment()
            self.assertTrue(utils.status_code_is_success(ft_resp.status_code))

            payment_log_count -= i+1
            self.assertEqual(utils.get_payment_log_count(), payment_log_count)

            # Check whether user was deleted
            if i >= 1:
                find_user1_resp = utils.find_user_benchmark(user1_id)
                self.assertTrue(utils.status_code_is_failure(find_user1_resp.status_code))

    def test_user_find_contains_faulty_log(self):
        # Get initial log count
        payment_log_count = int(utils.get_payment_log_count())
        self.assertIsNotNone(payment_log_count)

        log_id = str(uuid.uuid4())

        # Create an entry for the receive from user log
        log1_resp = utils.create_payment_log(
            log_id=log_id,
            type=LogType.RECEIVED,
            from_url="BENCHMARK",
            to_url=f"{utils.PAYMENT_URL}/payment/find_user/{log_id}",
            user_id=str(uuid.uuid4()),
            status=LogStatus.PENDING,
        )
        self.assertTrue(utils.status_code_is_success(log1_resp.status_code))

        payment_log_count += 1
        self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

        ft_resp = utils.fault_tolerance_payment()
        self.assertTrue(utils.status_code_is_success(ft_resp.status_code))

        payment_log_count -= 1
        self.assertEqual(utils.get_payment_log_count(), payment_log_count)

    def test_add_funds_contains_faulty_log(self):
        # Get initial log count
        payment_log_count = int(utils.get_payment_log_count())
        credit = 5
        self.assertIsNotNone(payment_log_count)

        for i in range(2):
            log_id = str(uuid.uuid4())

            user_entry = utils.create_user_benchmark()
            self.assertTrue(utils.status_code_is_success(user_entry.status_code))

            user_id = user_entry.json()['user_id']
            endpoint_url = f"{utils.PAYMENT_URL}/payment/add_funds/{user_id}/{credit}"

            # Create an entry for the receive from user log
            if i >= 0:
                log1_resp = utils.create_payment_log(
                    log_id=log_id,
                    type=LogType.RECEIVED,
                    from_url="BENCHMARK",
                    to_url=endpoint_url,
                    user_id=user_id,
                    status=LogStatus.PENDING,
                )
                self.assertTrue(utils.status_code_is_success(log1_resp.status_code))

                payment_log_count += 1
                self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

            # Update the stock of the item
            if i >= 1:
                add_funds_resp = utils.add_credit_to_user_benchmark(user_id, credit)
                self.assertTrue(utils.status_code_is_success(add_funds_resp.status_code))

                find_user_resp = utils.find_user_benchmark(user_id)
                self.assertTrue(utils.status_code_is_success(find_user_resp.status_code))
                self.assertEqual(find_user_resp.json()['credit'], credit)

            # Create an entry for the update log
            if i >= 1:
                log2_resp = utils.create_payment_log(
                    log_id=log_id,
                    type=LogType.UPDATE,
                    user_id=user_id,
                    old_uservalue=UserValue(credit=credit),
                    new_uservalue=UserValue(credit=credit*2),
                )
                self.assertTrue(utils.status_code_is_success(log2_resp.status_code))

                payment_log_count += 1
                self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

            ft_resp = utils.fault_tolerance_payment()
            self.assertTrue(utils.status_code_is_success(ft_resp.status_code))

            payment_log_count -= i+1
            self.assertEqual(utils.get_payment_log_count(), payment_log_count)

    def test_pay_contains_faulty_log(self):
        # Get initial log count
        payment_log_count = int(utils.get_payment_log_count())
        self.assertIsNotNone(payment_log_count)

        for i in range(2):
            log_id = str(uuid.uuid4())
            credit = 5

            user_entry = utils.create_user_benchmark()

            self.assertTrue(utils.status_code_is_success(user_entry.status_code))

            user_id = user_entry.json()['user_id']

            endpoint_url = f"{utils.PAYMENT_URL}/payment/pay/{user_id}/{credit}"

            add_funds_resp = utils.add_credit_to_user_benchmark(user_id, credit)
            self.assertTrue(utils.status_code_is_success(add_funds_resp.status_code))

            # Create an entry for the receive from user log
            if i >= 0:
                log1_resp = utils.create_payment_log(
                    log_id=log_id,
                    type=LogType.RECEIVED,
                    from_url="BENCHMARK",
                    to_url=endpoint_url,
                    user_id=user_id,
                    status=LogStatus.PENDING,
                )
                self.assertTrue(utils.status_code_is_success(log1_resp.status_code))

                payment_log_count += 1
                self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

            # Update the stock of the item
            if i >= 1:
                pay_resp = utils.payment_pay_benchmark(user_id, credit)
                self.assertTrue(utils.status_code_is_success(pay_resp.status_code))

                find_user_resp = utils.find_user_benchmark(user_id)
                self.assertTrue(utils.status_code_is_success(find_user_resp.status_code))
                self.assertEqual(find_user_resp.json()['credit'], 0)

            # Create an entry for the update log
            if i >= 1:
                log2_resp = utils.create_payment_log(
                    log_id=log_id,
                    type=LogType.UPDATE,
                    user_id=user_id,
                    old_uservalue=UserValue(credit=credit),
                    new_uservalue=UserValue(credit=0),
                )
                self.assertTrue(utils.status_code_is_success(log2_resp.status_code))

                payment_log_count += 1
                self.assertEqual(int(utils.get_payment_log_count()), payment_log_count)

            ft_resp = utils.fault_tolerance_payment()
            self.assertTrue(utils.status_code_is_success(ft_resp.status_code))

            payment_log_count -= i+1
            self.assertEqual(utils.get_payment_log_count(), payment_log_count)


if __name__ == '__main__':
    unittest.main()
