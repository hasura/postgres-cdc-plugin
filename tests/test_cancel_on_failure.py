import logging
import unittest
from psycopg2 import DatabaseError
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestCancelOnFailure(BaseTestCase):

    def test_webhook_cancel_on_failure(self):
        logger.info("Testing webhook behavior with cancel_on_failure=true...")

        # Set a significant delay on the webhook server to simulate a slow response
        self.webhook_server.response_delay = 5  # Delay responses by 5 seconds

        # Configure the trigger with cancel_on_failure = true
        retry_number = 2
        retry_interval = 2  # Interval between retries
        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := %s,
                table_name := %s,
                operations := %s::text[],
                webhook_url := 'http://host.docker.internal:8000/webhook',
                headers := '{"X-API-Key": "test-key"}'::jsonb,
                timeout := 1,  -- Trigger timeout of 1 second
                retry_number := %s,
                retry_interval := %s,
                retry_backoff := 'LINEAR',
                cancel_on_failure := true
            );
            """,
            ("employee_cancel_test", "employees", ["INSERT"], retry_number, retry_interval)
        )
        logger.info("Trigger created with cancel_on_failure=true.")

        # Attempt to insert a record to trigger the webhook
        with self.assertRaises(DatabaseError, msg="Transaction should fail due to webhook failure."):
            self.db.cur.execute("INSERT INTO employees (name, salary) VALUES (%s, %s);", ("Cancel Test", 70000))

        logger.info("Transaction was canceled as expected.")

        # Verify that no new record was added to the table
        self.db.cur.execute("SELECT COUNT(*) FROM employees;")
        employee_count = self.db.cur.fetchone()[0]
        self.assertEqual(employee_count, 0, "No record should exist as the transaction should have been canceled.")

        # Verify that webhook attempts were made
        webhook_received_count = len(self.webhook_server.received_webhooks)
        self.assertGreaterEqual(
            webhook_received_count, 1,
            msg="Expected at least one webhook attempt, but none were received."
        )

        # Verify the webhook payload
        webhook = self.webhook_server.received_webhooks[0]
        self.assertEqual(webhook["event"]["op"], "INSERT")
        self.assertEqual(webhook["event"]["data"]["new"]["name"], "Cancel Test")
        logger.info("Webhook payload validated successfully.")

        # Verify no further retries occurred after the transaction cancellation
        self.assertLessEqual(
            webhook_received_count, retry_number + 1,
            msg=f"Webhook retries exceeded expected count. Expected at most {retry_number + 1}, got {webhook_received_count}."
        )
        logger.info("Transaction cancellation and webhook behavior validated successfully.")


if __name__ == "__main__":
    unittest.main()
