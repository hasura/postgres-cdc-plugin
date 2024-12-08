import logging
import unittest
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestSecurityModes(BaseTestCase):
    def test_security_mode_private(self):
        logger.info("Testing security mode 'PRIVATE'...")

        # Step 1: Create a trigger in PRIVATE mode
        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                name := %s,
                table_name := %s,
                operations := %s::text[],
                webhook_url := %s,
                headers := %s::jsonb,
                security := 'PRIVATE'
            );
            """,
            (
                "employee_private_security",
                "employees",
                ["INSERT"],
                "http://host.docker.internal:8000/webhook",
                '{"X-API-Key": "test-key"}',
            ),
        )
        logger.info("Trigger created with security mode 'PRIVATE'.")

        # Step 2: Verify credentials are securely stored in the credentials table
        self.db.cur.execute(
            """
            SELECT trigger_schema, trigger_table, trigger_name, webhook_url, headers
            FROM cdc_webhook.credentials
            WHERE trigger_name = %s;
            """,
            ("employee_private_security",),
        )
        credentials = self.db.cur.fetchone()
        logger.info("Fetched credentials: %s", credentials)
        self.assertIsNotNone(credentials, "Credentials should be stored in 'PRIVATE' mode.")
        self.assertEqual(credentials[3], "http://host.docker.internal:8000/webhook")
        self.assertEqual(credentials[4], {"X-API-Key": "test-key"})
        logger.info("Credentials securely stored and validated.")

        # Step 3: Verify the trigger and log its definition
        self.db.cur.execute("SELECT tgname, tgfoid::regproc::text FROM pg_trigger;")
        triggers = self.db.cur.fetchall()
        logger.info("Existing triggers: %s", triggers)
        trigger_name = "employee_private_security"
        self.assertTrue(any(t[0] == trigger_name for t in triggers), "Trigger not found.")

        self.db.cur.execute(
            """
            SELECT pg_get_triggerdef(oid)
            FROM pg_trigger
            WHERE tgname = %s;
            """,
            (trigger_name,),
        )
        trigger_definition = self.db.cur.fetchone()
        self.assertIsNotNone(trigger_definition, "Trigger definition not found.")
        logger.info(f"Trigger definition:\n{trigger_definition[0]}")
        self.assertNotIn("http://host.docker.internal:8000/webhook", trigger_definition[0])
        self.assertNotIn("test-key", trigger_definition[0])
        logger.info("Trigger definition does not expose sensitive information.")

        # Step 4: Fetch and log the function definition
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
        logger.info("Function definition:\n%s", function_definition[0])

        # Step 5: Perform an insert operation and validate webhook delivery
        self.db.cur.execute("INSERT INTO employees (name, salary) VALUES (%s, %s);", ("Private Security", 60000))
        webhook = self._wait_for_webhook()
        self.assertEqual(webhook["event"]["op"], "INSERT")
        self.assertEqual(webhook["event"]["data"]["new"]["name"], "Private Security")
        logger.info("Webhook validated successfully for 'PRIVATE' security mode.")


if __name__ == "__main__":
    unittest.main()
