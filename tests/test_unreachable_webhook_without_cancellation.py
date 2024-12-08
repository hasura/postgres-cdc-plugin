import logging
import unittest
from psycopg2 import DatabaseError
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestUnreachableWebhookURLWithoutCancel(BaseTestCase):

    def test_unreachable_webhook_without_cancellation(self):
        logger.info("Testing unreachable webhook URL with cancel_on_failure=false...")

        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := 'unreachable_no_cancel',
                table_name := 'employees',
                operations := ARRAY['INSERT'],
                webhook_url := 'http://nonexistent.webhook.url:9999/',
                headers := '{"X-API-Key": "test-key"}'::jsonb,
                cancel_on_failure := false,
                retry_number := 0
            );
            """
        )
        logger.info("Trigger created for unreachable URL without cancellation.")

        # Insert should succeed despite unreachable webhook, but log a warning
        self.db.cur.execute("INSERT INTO employees (name, salary) VALUES ('NoCancel', 45000);")

        # Confirm record is inserted
        self.db.cur.execute("SELECT COUNT(*) FROM employees WHERE name = 'NoCancel';")
        count = self.db.cur.fetchone()[0]
        self.assertEqual(count, 1, "Record should be present despite webhook failure, due to no cancellation.")
        logger.info("Transaction committed despite unreachable webhook, as expected.")


if __name__ == "__main__":
    unittest.main()
