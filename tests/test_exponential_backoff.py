import time
import logging
import unittest
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestExponentialBackoff(BaseTestCase):
    def test_exponential_backoff(self):
        logger.info("Testing exponential backoff...")

        # Set webhook server delay to ensure initial attempts fail
        self.webhook_server.response_delay = 3

        retry_number = 2
        retry_interval = 1
        # Attempts (total): first + 2 retries = 3 attempts
        # With exponential backoff: attempts at ~0s, ~1s later, ~2s after that (2^n)
        # Actual timing might vary slightly; we focus on number of attempts.

        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := 'employee_exponential_test',
                table_name := 'employees',
                operations := ARRAY['INSERT'],
                webhook_url := 'http://host.docker.internal:8000/webhook',
                headers := '{"X-API-Key": "test-key"}'::jsonb,
                timeout := 1,
                retry_number := %s,
                retry_interval := %s,
                retry_backoff := 'EXPONENTIAL',
                cancel_on_failure := false
            );
            """,
            (retry_number, retry_interval)
        )
        logger.info("Trigger with exponential backoff created.")

        # Insert a record
        self.db.cur.execute("INSERT INTO employees (name, salary) VALUES ('ExpBackoff', 70000);")

        # After the first failed attempt, we expect retries.
        # Wait enough time for all attempts (0s first attempt, then 1s, then 2s)
        time.sleep(10)  # More than enough time for all attempts

        attempts = len(self.webhook_server.received_webhooks)
        self.assertEqual(
            attempts, retry_number + 1,
            f"Expected {retry_number + 1} attempts (1 original + {retry_number} retries), got {attempts}."
        )

        # Verify that final attempt succeeded (due to delayed server response)
        webhook = self.webhook_server.received_webhooks[-1]
        self.assertEqual(webhook["event"]["op"], "INSERT")
        self.assertEqual(webhook["event"]["data"]["new"]["name"], "ExpBackoff")
        logger.info("Exponential backoff attempts validated successfully.")


if __name__ == "__main__":
    unittest.main()
