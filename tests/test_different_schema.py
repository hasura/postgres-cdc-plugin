import logging
import unittest
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestTriggerWithDifferentSchema(BaseTestCase):
    def test_trigger_in_custom_schema(self):
        logger.info("Testing trigger in a custom schema...")

        # Create a custom schema and table
        logger.info("Creating custom schema and table...")
        self.db.cur.execute("CREATE SCHEMA IF NOT EXISTS hr;")
        self.db.cur.execute("""
            CREATE TABLE IF NOT EXISTS hr.employees (
                id SERIAL PRIMARY KEY,
                name TEXT,
                salary INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.info("Custom schema and table created.")

        # Create the trigger for the custom schema table
        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := %s,
                table_name := %s,
                operations := %s::text[],
                webhook_url := 'http://host.docker.internal:8000/webhook',
                headers := '{"X-API-Key": "test-key"}'::jsonb,
                schema_name := %s
            );
            """,
            ("hr_employee_changes", "employees", ["INSERT", "UPDATE", "DELETE"], "hr")
        )
        logger.info("Trigger for custom schema table created successfully.")

        # Test INSERT operation
        logger.info("Testing INSERT operation in custom schema...")
        self.db.cur.execute("INSERT INTO hr.employees (name, salary) VALUES (%s, %s);", ("Alice", 70000))
        webhook = self._wait_for_webhook()
        self.assertEqual(webhook["event"]["op"], "INSERT")
        self.assertEqual(webhook["event"]["data"]["new"]["name"], "Alice")
        logger.info("INSERT operation passed in custom schema.")

        # Test UPDATE operation
        logger.info("Testing UPDATE operation in custom schema...")
        self.db.cur.execute("UPDATE hr.employees SET salary = %s WHERE name = %s;", (75000, "Alice"))
        webhook = self._wait_for_webhook(start_index=1)
        self.assertEqual(webhook["event"]["op"], "UPDATE")
        self.assertEqual(webhook["event"]["data"]["new"]["salary"], 75000)
        self.assertEqual(webhook["event"]["data"]["old"]["salary"], 70000)
        logger.info("UPDATE operation passed in custom schema.")

        # Test DELETE operation
        logger.info("Testing DELETE operation in custom schema...")
        self.db.cur.execute("DELETE FROM hr.employees WHERE name = %s;", ("Alice",))
        webhook = self._wait_for_webhook(start_index=2)
        self.assertEqual(webhook["event"]["op"], "DELETE")
        self.assertEqual(webhook["event"]["data"]["old"]["name"], "Alice")
        self.assertIsNone(webhook["event"]["data"]["new"])
        logger.info("DELETE operation passed in custom schema.")

        # Cleanup the custom schema
        logger.info("Cleaning up custom schema...")
        self.db.cur.execute("DROP SCHEMA hr CASCADE;")
        logger.info("Custom schema cleanup complete.")


if __name__ == "__main__":
    unittest.main()
