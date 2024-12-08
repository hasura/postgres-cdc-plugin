import json
import logging
import unittest
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestBasicInsert(BaseTestCase):

    def test_insert_trigger(self):
        logger.info("Testing INSERT trigger...")

        # Creating the trigger as part of the test logic
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
            ("employee_insert", "employees", ["INSERT"])
        )
        logger.info("Trigger created successfully.")

        # Performing the test action
        logger.info("Inserting record into 'employees' table...")
        self.db.cur.execute(
            "INSERT INTO employees (name, salary) VALUES (%s, %s);",
            ("Alice", 75000)
        )

        # Verifying the webhook
        webhook = self._wait_for_webhook()
        logger.info(f"Validating webhook: {json.dumps(webhook, indent=2)}")
        self.assertEqual(webhook["event"]["op"], "INSERT")
        self.assertEqual(webhook["event"]["data"]["new"]["name"], "Alice")
        self.assertEqual(webhook["event"]["data"]["new"]["salary"], 75000)
        self.assertEqual(webhook["event"]["data"]["old"], None)
        logger.info("INSERT trigger test passed.")


if __name__ == "__main__":
    unittest.main()
