import logging
import unittest
from psycopg2 import DatabaseError
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestUnreachableWebhookURLWithCancel(BaseTestCase):
    def test_unreachable_webhook_with_cancellation(self):
        logger.info("Testing unreachable webhook URL with cancel_on_failure=true...")

        # Using a non-routable URL
        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := 'unreachable_with_cancel',
                table_name := 'employees',
                operations := ARRAY['INSERT'],
                webhook_url := 'http://nonexistent.webhook.url:9999/',
                headers := '{"X-API-Key": "test-key"}'::jsonb,
                cancel_on_failure := true,
                retry_number := 0
            );
            """
        )
        logger.info("Trigger created for unreachable URL with cancellation.")

        # Insert should fail due to unreachable webhook
        with self.assertRaises(DatabaseError):
            self.db.cur.execute("INSERT INTO employees (name, salary) VALUES ('NoHost', 40000);")

        # Confirm no record is inserted
        self.db.cur.execute("SELECT COUNT(*) FROM employees;")
        count = self.db.cur.fetchone()[0]
        self.assertEqual(count, 0, "No records should be present as transaction should have been canceled.")
        logger.info("Transaction rolled back as expected.")


if __name__ == "__main__":
    unittest.main()
