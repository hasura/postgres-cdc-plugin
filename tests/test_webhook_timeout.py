import time
import logging
import unittest
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestWebhookTimeout(BaseTestCase):
    def test_webhook_timeout(self):
        logger.info("Testing webhook timeout behavior...")

        # Set a delay on the webhook server to simulate a delayed response
        self.webhook_server.response_delay = 3  # Server will delay responses by 5 seconds

        # Create the trigger with a timeout shorter than the server's delay
        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := %s,
                table_name := %s,
                operations := %s::text[],
                webhook_url := 'http://host.docker.internal:8000/webhook',
                headers := '{"X-API-Key": "test-key"}'::jsonb,
                timeout := 2,  -- Trigger timeout of 2 seconds
                retry_number := 0  -- Disable retries
            );
            """,
            ("employee_timeout_test", "employees", ["INSERT"])
        )
        logger.info("Trigger created with a 2-second timeout.")
        # Measure the time taken to execute the insert
        start_time = time.time()
        self.db.cur.execute("INSERT INTO employees (name, salary) VALUES (%s, %s);", ("Timeout Test", 10000))
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"Database operation took {elapsed_time:.2f} seconds.")

        # Assert that the elapsed time is approximately equal to the timeout
        self.assertAlmostEqual(
            elapsed_time, 2, delta=0.5,
            msg=f"Database operation did not time out as expected. Took {elapsed_time:.2f} seconds."
        )

        # Verify that the webhook is NOT received within the timeout period
        try:
            self._wait_for_webhook(timeout=0)
            self.fail("Webhook was unexpectedly received within the timeout period.")
        except AssertionError:
            logger.info("No webhook received within the timeout period, as expected.")

        # Verify that the webhook is received after the timeout period
        webhook_received = False
        try:
            webhook = self._wait_for_webhook(timeout=10)  # Allow sufficient time for delayed webhook
            webhook_received = True
            logger.info("Webhook received after the timeout, as expected.")
            if webhook_received:
                self.assertEqual(webhook["event"]["op"], "INSERT")
                self.assertEqual(webhook["event"]["data"]["new"]["name"], "Timeout Test")
                logger.info("Webhook content validated successfully.")
        except AssertionError:
            logger.error("Expected webhook was not received after the timeout.")

        self.assertTrue(webhook_received, "Webhook was not received after the timeout.")


if __name__ == "__main__":
    unittest.main()
