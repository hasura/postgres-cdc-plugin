import logging
import unittest
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestSecurityModes(BaseTestCase):
    def test_security_mode_none(self):
        logger.info("Testing security mode 'NONE'...")

        # Step 1: Create a trigger in NONE mode
        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := %s,
                table_name := %s,
                operations := %s::text[],
                webhook_url := %s,
                headers := %s::jsonb,
                security := 'NONE'
            );
            """,
            (
                "employee_none_security",
                "employees",
                ["INSERT"],
                "http://host.docker.internal:8000/webhook",
                '{"X-API-Key": "test-key"}',
            ),
        )
        logger.info("Trigger created with security mode 'NONE'.")

        # Step 2: Verify that no credentials are stored in the credentials table
        self.db.cur.execute(
            "SELECT * FROM cdc_webhook.credentials WHERE trigger_name = %s;",
            ("employee_none_security",),
        )
        credentials = self.db.cur.fetchall()
        logger.info("Fetched credentials: %s", credentials)
        self.assertEqual(len(credentials), 0, "No credentials should be stored in 'NONE' mode.")

        # Step 3: Verify the trigger and associated function
        self.db.cur.execute("SELECT tgname, tgfoid::regproc::text FROM pg_trigger;")
        triggers = self.db.cur.fetchall()
        logger.info("Existing triggers: %s", triggers)
        trigger_name = "employee_none_security"
        self.assertTrue(any(t[0] == trigger_name for t in triggers), "Trigger not found.")

        self.db.cur.execute(
            """
            SELECT tgfoid::regproc::text
            FROM pg_trigger
            WHERE tgname = %s;
            """,
            (trigger_name,),
        )
        function_name = self.db.cur.fetchone()
        self.assertIsNotNone(function_name, "Trigger function not found.")
        logger.info("Function name associated with trigger: %s", function_name[0])

        # Step 4: Verify the function definition contains credentials
        self.db.cur.execute(
            """
            SELECT pg_get_functiondef(oid)
            FROM pg_proc
            WHERE proname = %s;
            """,
            (function_name[0].strip('"'),),  # Remove quotes if present
        )
        function_definition = self.db.cur.fetchone()
        self.assertIsNotNone(function_definition, "Function definition not found.")
        self.assertIn("http://host.docker.internal:8000/webhook", function_definition[0])
        self.assertIn('"X-API-Key": "test-key"', function_definition[0])
        logger.info("Webhook URL and headers are visible in the function definition for 'NONE' mode.")

        # Step 5: Perform an insert operation and validate webhook delivery
        self.db.cur.execute("INSERT INTO employees (name, salary) VALUES (%s, %s);", ("None Security", 50000))
        webhook = self._wait_for_webhook()
        self.assertEqual(webhook["event"]["op"], "INSERT")
        self.assertEqual(webhook["event"]["data"]["new"]["name"], "None Security")
        logger.info("Webhook validated successfully for 'NONE' security mode.")


if __name__ == "__main__":
    unittest.main()
