import logging
import unittest
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestFullColumnUpdateTracking(BaseTestCase):
    def test_full_column_tracking(self):
        logger.info("Testing update trigger with all column tracking...")

        # Assume employees table has columns: id, name, salary, created_at
        # Track all columns: id, name, salary, created_at
        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := 'employee_all_columns_update',
                table_name := 'employees',
                operations := ARRAY['UPDATE'],
                webhook_url := 'http://host.docker.internal:8000/webhook',
                headers := '{"X-API-Key": "test-key"}'::jsonb,
                update_columns := ARRAY['id','name','salary','created_at']::text[]
            );
            """
        )
        logger.info("Trigger for all column updates created successfully.")

        # Insert a record
        self.db.cur.execute("INSERT INTO employees (name, salary) VALUES ('FullTrack', 50000);")

        # Update the name (non-salary column)
        start_index = len(self.webhook_server.received_webhooks)
        self.db.cur.execute("UPDATE employees SET name = 'FullTrackUpdated' WHERE name = 'FullTrack';")
        webhook = self._wait_for_webhook(start_index=start_index)
        self.assertEqual(webhook["event"]["op"], "UPDATE")
        self.assertEqual(webhook["event"]["data"]["old"]["name"], "FullTrack")
        self.assertEqual(webhook["event"]["data"]["new"]["name"], "FullTrackUpdated")
        logger.info("UPDATE triggered due to name change.")

        # Update the salary column
        start_index = len(self.webhook_server.received_webhooks)
        self.db.cur.execute("UPDATE employees SET salary = 55000 WHERE name = 'FullTrackUpdated';")
        webhook = self._wait_for_webhook(start_index=start_index)
        self.assertEqual(webhook["event"]["data"]["old"]["salary"], 50000)
        self.assertEqual(webhook["event"]["data"]["new"]["salary"], 55000)
        logger.info("UPDATE triggered due to salary change.")

        # Even updating a timestamp or any other column should trigger webhook
        # (If we had a column we could manually set, we would do so here.)
        logger.info("All column tracking test passed successfully.")


if __name__ == "__main__":
    unittest.main()
