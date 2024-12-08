import json
import logging
import unittest
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestUpdateTrigger(BaseTestCase):

    def test_update_trigger_with_column_tracking(self):
        logger.info("Testing UPDATE trigger with column tracking...")

        # Create the trigger for UPDATE operation, tracking only 'salary' column
        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := %s,
                table_name := %s,
                operations := %s::text[],
                webhook_url := 'http://host.docker.internal:8000/webhook',
                headers := '{"X-API-Key": "test-key"}'::jsonb,
                update_columns := %s::text[]
            );
            """,
            ("employee_update_salary", "employees", ["UPDATE"], ["salary"])
        )
        logger.info("Trigger created successfully.")

        # Insert an initial record
        self.db.cur.execute("INSERT INTO employees (name, salary) VALUES (%s, %s);", ("Derek", 50000))

        # Record current webhook count
        start_index = len(self.webhook_server.received_webhooks)

        # Update tracked column (triggers webhook)
        logger.info("Updating 'salary' column in 'employees' table...")
        self.db.cur.execute("UPDATE employees SET salary = %s WHERE name = %s;", (55000, "Derek"))

        # Verify webhook payload
        webhook = self._wait_for_webhook(start_index=start_index)
        logger.info(f"Validating webhook: {json.dumps(webhook, indent=2)}")
        self.assertEqual(webhook["event"]["op"], "UPDATE")
        self.assertEqual(webhook["event"]["data"]["new"]["salary"], 55000)
        self.assertEqual(webhook["event"]["data"]["old"]["salary"], 50000)
        logger.info("UPDATE trigger test passed.")

        # Update non-tracked column (should NOT trigger webhook)
        logger.info("Updating 'name' column in 'employees' table...")
        self.db.cur.execute("UPDATE employees SET name = %s WHERE salary = %s;", ("Derek Updated", 55000))

        # Ensure no new webhook is triggered
        with self.assertRaises(AssertionError, msg="Webhook triggered for non-tracked column"):
            self._wait_for_webhook(start_index=start_index + 1, timeout=2)
        logger.info("Non-tracked column update did not trigger webhook as expected.")


if __name__ == "__main__":
    unittest.main()
