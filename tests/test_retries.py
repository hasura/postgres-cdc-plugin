import time
import logging
import unittest
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestTimeoutAndRetriesWithoutCancel(BaseTestCase):

    def test_webhook_timeout_and_retries_no_rollback(self):
        logger.info("Testing webhook timeout and retries without rollback...")

        # Set a significant delay on the webhook server to simulate a slow response
        self.webhook_server.response_delay = 5  # Delay responses by 5 seconds

        # Configure the trigger with a shorter timeout and retries, without canceling the transaction
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
                cancel_on_failure := false
            );
            """,
            ("employee_retry_no_rollback", "employees", ["INSERT"], retry_number, retry_interval)
        )
        logger.info("Trigger created with timeout of 1 second, 2 retries, and no rollback on failure.")

        # Insert a record to trigger the webhook
        self.db.cur.execute("INSERT INTO employees (name, salary) VALUES (%s, %s);", ("Delayed Retry", 60000))
        logger.info("Transaction completed successfully, despite webhook delay.")

        # Wait for the server delay to complete (allow first webhook attempt)
        time.sleep(self.webhook_server.response_delay)

        # Verify that retries started as expected
        webhook_received_count = len(self.webhook_server.received_webhooks)
        self.assertGreaterEqual(
            webhook_received_count, 1,
            msg="Expected at least 1 webhook attempt by now, but none were received."
        )

        # Wait for retries to complete and validate
        total_retry_wait = retry_number * retry_interval
        time.sleep(total_retry_wait)  # Wait only for the remaining retries

        # Validate the total number of webhook attempts
        webhook_received_count = len(self.webhook_server.received_webhooks)
        self.assertEqual(
            webhook_received_count, retry_number + 1,
            msg=f"Webhook retries did not match expected count. Expected {retry_number + 1}, got {webhook_received_count}."
        )

        # Verify the content of the last webhook attempt
        webhook = self.webhook_server.received_webhooks[-1]
        self.assertEqual(webhook["event"]["op"], "INSERT")
        self.assertEqual(webhook["event"]["data"]["new"]["name"], "Delayed Retry")
        logger.info("Webhook retries and delayed completion validated successfully.")


if __name__ == "__main__":
    unittest.main()