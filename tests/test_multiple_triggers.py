import logging
import unittest
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestMultipleTriggersSameTable(BaseTestCase):
    def test_multiple_triggers_on_same_table(self):
        logger.info("Testing multiple triggers on the same table...")

        # Create first trigger for INSERT operations only
        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := 'employee_insert_only',
                table_name := 'employees',
                operations := ARRAY['INSERT'],
                webhook_url := 'http://host.docker.internal:8000/webhook',
                headers := '{"X-API-Key": "insert-only-key"}'::jsonb
            );
            """
        )
        logger.info("Created insert-only trigger.")

        # Create second trigger for UPDATE and DELETE operations
        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := 'employee_update_delete',
                table_name := 'employees',
                operations := ARRAY['UPDATE', 'DELETE'],
                webhook_url := 'http://host.docker.internal:8000/webhook',
                headers := '{"X-API-Key": "update-delete-key"}'::jsonb
            );
            """
        )
        logger.info("Created update/delete trigger.")

        # Test INSERT operation (should only fire insert-only trigger)
        self.db.cur.execute("INSERT INTO employees (name, salary) VALUES ('MultiTrigger', 50000);")
        insert_webhook = self._wait_for_webhook()
        self.assertEqual(insert_webhook["event"]["op"], "INSERT")
        self.assertEqual(insert_webhook["trigger"]["name"], "employee_insert_only")
        logger.info("INSERT operation fired insert-only trigger as expected.")

        # Test UPDATE operation (should only fire update/delete trigger)
        start_index = len(self.webhook_server.received_webhooks)
        self.db.cur.execute("UPDATE employees SET salary = 60000 WHERE name = 'MultiTrigger';")
        update_webhook = self._wait_for_webhook(start_index=start_index)
        self.assertEqual(update_webhook["event"]["op"], "UPDATE")
        self.assertEqual(update_webhook["trigger"]["name"], "employee_update_delete")
        logger.info("UPDATE operation fired update/delete trigger as expected.")

        # Test DELETE operation (should only fire update/delete trigger)
        start_index = len(self.webhook_server.received_webhooks)
        self.db.cur.execute("DELETE FROM employees WHERE name = 'MultiTrigger';")
        delete_webhook = self._wait_for_webhook(start_index=start_index)
        self.assertEqual(delete_webhook["event"]["op"], "DELETE")
        self.assertEqual(delete_webhook["trigger"]["name"], "employee_update_delete")
        logger.info("DELETE operation fired update/delete trigger as expected.")


if __name__ == "__main__":
    unittest.main()
