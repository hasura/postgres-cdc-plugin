import logging
import unittest
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestAllOperationsTrigger(BaseTestCase):
    def test_all_operations_trigger(self):
        logger.info("Testing trigger with all operations (INSERT, UPDATE, DELETE)...")

        # Create a trigger for all operations
        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := %s,
                table_name := %s,
                operations := %s::text[],
                webhook_url := 'http://host.docker.internal:8000/webhook',
                headers := '{"X-API-Key": "test-key"}'::jsonb
            );
            """,
            ("employee_all_ops", "employees", ["INSERT", "UPDATE", "DELETE"])
        )
        logger.info("Trigger for all operations created successfully.")

        # Test INSERT operation
        logger.info("Testing INSERT operation...")
        self.db.cur.execute("INSERT INTO employees (name, salary) VALUES (%s, %s);", ("John Doe", 60000))
        webhook = self._wait_for_webhook()
        self.assertEqual(webhook["event"]["op"], "INSERT")
        self.assertEqual(webhook["event"]["data"]["new"]["name"], "John Doe")
        logger.info("INSERT operation passed.")

        # Test UPDATE operation
        logger.info("Testing UPDATE operation...")
        self.db.cur.execute("UPDATE employees SET salary = %s WHERE name = %s;", (65000, "John Doe"))
        webhook = self._wait_for_webhook(start_index=1)
        self.assertEqual(webhook["event"]["op"], "UPDATE")
        self.assertEqual(webhook["event"]["data"]["new"]["salary"], 65000)
        self.assertEqual(webhook["event"]["data"]["old"]["salary"], 60000)
        logger.info("UPDATE operation passed.")

        # Test DELETE operation
        logger.info("Testing DELETE operation...")
        self.db.cur.execute("DELETE FROM employees WHERE name = %s;", ("John Doe",))
        webhook = self._wait_for_webhook(start_index=2)
        self.assertEqual(webhook["event"]["op"], "DELETE")
        self.assertEqual(webhook["event"]["data"]["old"]["name"], "John Doe")
        self.assertIsNone(webhook["event"]["data"]["new"])
        logger.info("DELETE operation passed.")


if __name__ == "__main__":
    unittest.main()
