import unittest
import psycopg2
from fastapi import FastAPI, Request
import uvicorn
import threading
import asyncio
import json
import time
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class PostgresConnection:
    """Manages the PostgreSQL database connection and setup."""
    host: str = "localhost"
    port: int = 5432
    dbname: str = "testdb"
    user: str = "postgres"
    password: str = ""
    conn = None
    cur = None

    def connect(self):
        """Establishes database connection and creates test tables."""
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def setup_test_environment(self):
        """Creates extension and test tables."""
        self.cur.execute("CREATE EXTENSION IF NOT EXISTS cdc_webhook;")
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                name TEXT,
                salary INTEGER,
                department TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

    def cleanup(self):
        """Removes test tables and extension."""
        if self.cur:
            self.cur.execute("DROP TABLE IF EXISTS employees CASCADE;")
            self.cur.execute("DROP EXTENSION IF EXISTS cdc_webhook CASCADE;")
            self.cur.close()
        if self.conn:
            self.conn.close()


@dataclass
class WebhookServer:
    """Manages the webhook server for testing."""
    host: str = "127.0.0.1"
    port: int = 8000
    app: FastAPI = field(default_factory=FastAPI)
    received_webhooks: List[Dict] = field(default_factory=list)
    server_thread: threading.Thread = None

    def start(self):
        """Starts the webhook server in a background thread."""

        @self.app.post("/webhook")
        async def webhook_endpoint(request: Request):
            payload = await request.json()
            self.received_webhooks.append(payload)
            return {"status": "success"}

        def run_server():
            config = uvicorn.Config(
                self.app,
                host=self.host,
                port=self.port,
                log_level="error"
            )
            uvicorn.Server(config).run()

        self.server_thread = threading.Thread(target=run_server, daemon=True)
        self.server_thread.start()
        self._wait_for_server()

    def _wait_for_server(self, timeout=5, interval=0.1):
        """Waits for the server to become available."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"http://{self.host}:{self.port}")
                if response.status_code in (404, 405):
                    return
            except requests.exceptions.ConnectionError:
                time.sleep(interval)
                continue
        raise RuntimeError("Webhook server failed to start")

    def clear_webhooks(self):
        """Clears recorded webhooks."""
        self.received_webhooks.clear()


class TestCDCWebhook(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Sets up testing environment."""
        cls.db = PostgresConnection()
        cls.db.connect()
        cls.db.setup_test_environment()

        cls.webhook_server = WebhookServer()
        cls.webhook_server.start()

    def setUp(self):
        """Prepares for each test."""
        self.db.cur.execute("TRUNCATE employees RESTART IDENTITY CASCADE;")
        self.webhook_server.clear_webhooks()

    def test_insert_notification(self):
        """Tests that INSERT operations trigger webhooks correctly."""
        self._create_trigger('employees', ['INSERT'])

        self.db.cur.execute(
            "INSERT INTO employees (name, salary) VALUES (%s, %s);",
            ('John Doe', 50000)
        )

        webhook = self._get_latest_webhook(timeout=2)
        self.assertEqual(webhook['event']['op'], 'INSERT')
        self.assertEqual(webhook['event']['data']['new']['name'], 'John Doe')
        self.assertEqual(webhook['event']['data']['new']['salary'], 50000)

    def test_update_with_tracked_columns(self):
        """Tests that updates to tracked columns trigger webhooks."""
        self._create_trigger(
            'employees',
            ['UPDATE'],
            update_columns=['salary']
        )

        # Initial insert
        self.db.cur.execute(
            "INSERT INTO employees (name, salary) VALUES (%s, %s);",
            ('Jane Smith', 60000)
        )

        # Update tracked column
        self.db.cur.execute(
            "UPDATE employees SET salary = %s WHERE name = %s;",
            (65000, 'Jane Smith')
        )

        webhook = self._get_latest_webhook(timeout=2)
        self.assertEqual(webhook['event']['op'], 'UPDATE')
        self.assertEqual(webhook['event']['data']['old']['salary'], 60000)
        self.assertEqual(webhook['event']['data']['new']['salary'], 65000)

    def test_update_untracked_column(self):
        """Tests that updates to untracked columns don't trigger webhooks."""
        self._create_trigger(
            'employees',
            ['UPDATE'],
            update_columns=['salary']
        )

        # Initial insert
        self.db.cur.execute(
            "INSERT INTO employees (name, salary) VALUES (%s, %s);",
            ('Bob Wilson', 70000)
        )

        # Update untracked column
        self.db.cur.execute(
            "UPDATE employees SET department = 'HR' WHERE name = %s;",
            ('Bob Wilson',)
        )

        self.assertEqual(len(self.webhook_server.received_webhooks), 0)

    def test_delete_notification(self):
        """Tests that DELETE operations trigger webhooks correctly."""
        self._create_trigger('employees', ['DELETE'])

        # Insert and then delete
        self.db.cur.execute(
            "INSERT INTO employees (name, salary) VALUES (%s, %s);",
            ('Alice Brown', 55000)
        )

        self.db.cur.execute(
            "DELETE FROM employees WHERE name = %s;",
            ('Alice Brown',)
        )

        webhook = self._get_latest_webhook(timeout=2)
        self.assertEqual(webhook['event']['op'], 'DELETE')
        self.assertEqual(webhook['event']['data']['old']['name'], 'Alice Brown')
        self.assertIsNone(webhook['event']['data']['new'])

    def _create_trigger(self, table_name: str, operations: List[str], **kwargs):
        """Creates a database trigger with specified configuration."""
        default_args = {
            'webhook_url': 'http://host.docker.internal:8000/webhook',  # Special Docker DNS name
            'headers': {'Content-Type': 'application/json'},
            'update_columns': [],
            'timeout': 5,
            'cancel_on_failure': False,
            'trigger_timing': 'AFTER',
            'retry_number': 3,
            'retry_interval': 1,
            'retry_backoff': 'LINEAR'
        }

        args = {**default_args, **kwargs}
        trigger_name = f"test_trigger_{table_name}"

        self.db.cur.execute(
            """
            SELECT create_event_trigger(
                %s, %s, %s::jsonb, %s::text[], %s,
                'public', %s::text[], %s, %s, %s, %s, %s, %s
            );
            """,
            (
                table_name,
                args['webhook_url'],
                json.dumps(args['headers']),
                operations,
                trigger_name,
                args['update_columns'],
                args['timeout'],
                args['cancel_on_failure'],
                args['trigger_timing'],
                args['retry_number'],
                args['retry_interval'],
                args['retry_backoff']
            )
        )

    def _get_latest_webhook(self, timeout=5) -> Dict[str, Any]:
        """Waits for and returns the most recent webhook."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.webhook_server.received_webhooks:
                return self.webhook_server.received_webhooks[-1]
            time.sleep(0.1)
        raise AssertionError(f"No webhook received within {timeout} seconds")

    @classmethod
    def tearDownClass(cls):
        """Cleans up the testing environment."""
        cls.db.cleanup()


if __name__ == '__main__':
    unittest.main(verbosity=2)
