import logging
import unittest
from psycopg2 import DatabaseError, ProgrammingError
from tests.utilities import BaseTestCase

logger = logging.getLogger(__name__)


class TestInvalidTriggerParameters(BaseTestCase):
    def test_invalid_operations(self):
        logger.info("Testing trigger creation with invalid operations array...")
        with self.assertRaises(DatabaseError):
            self.db.cur.execute("""
                SELECT create_event_trigger(
                    name := 'invalid_ops',
                    table_name := 'employees',
                    operations := ARRAY[]::text[],  -- empty array
                    webhook_url := 'http://host.docker.internal:8000/webhook'
                );
            """)
        logger.info("Invalid operations array test passed.")

    def test_invalid_timing(self):
        logger.info("Testing trigger creation with invalid timing...")
        with self.assertRaises(DatabaseError):
            self.db.cur.execute("""
                SELECT create_event_trigger(
                    name := 'invalid_timing',
                    table_name := 'employees',
                    operations := ARRAY['INSERT'],
                    webhook_url := 'http://host.docker.internal:8000/webhook',
                    trigger_timing := 'DURING'  -- invalid timing
                );
            """)
        logger.info("Invalid timing test passed.")

    def test_negative_retry_number(self):
        logger.info("Testing trigger creation with negative retry_number...")
        with self.assertRaises(DatabaseError):
            self.db.cur.execute("""
                SELECT create_event_trigger(
                    name := 'negative_retry',
                    table_name := 'employees',
                    operations := ARRAY['INSERT'],
                    webhook_url := 'http://host.docker.internal:8000/webhook',
                    retry_number := -1
                );
            """)
        logger.info("Negative retry_number test passed.")

    def test_zero_interval(self):
        logger.info("Testing trigger creation with zero retry_interval...")
        with self.assertRaises(DatabaseError):
            self.db.cur.execute("""
                SELECT create_event_trigger(
                    name := 'zero_interval',
                    table_name := 'employees',
                    operations := ARRAY['INSERT'],
                    webhook_url := 'http://host.docker.internal:8000/webhook',
                    retry_interval := 0
                );
            """)
        logger.info("Zero interval test passed.")


if __name__ == "__main__":
    unittest.main()
