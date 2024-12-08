from typing import List, Dict, Any
import requests
from fastapi import FastAPI, Request
import threading
import time
import psycopg2
import logging
import unittest

logger = logging.getLogger(__name__)


class PostgresConnection:
    def __init__(self, host="localhost", port=5432, dbname="testdb", user="postgres", password=""):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = None
        self.cur = None

    def connect(self):
        logger.info("Connecting to PostgreSQL database...")
        self.conn = psycopg2.connect(
            host=self.host, port=self.port, dbname=self.dbname, user=self.user, password=self.password
        )
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        logger.info("Connected to the database.")

    def close(self):
        logger.info("Closing database connection...")
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed.")

    def setup_environment(self):
        logger.info("Setting up database environment...")
        self.cur.execute("CREATE EXTENSION IF NOT EXISTS cdc_webhook;")
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS employees (
                id SERIAL PRIMARY KEY,
                name TEXT,
                salary INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.info("Database environment setup complete.")

    def cleanup(self):
        logger.info("Cleaning up database...")
        self.cur.execute("DROP TABLE IF EXISTS employees CASCADE;")
        self.cur.execute("DROP EXTENSION IF EXISTS cdc_webhook CASCADE;")
        logger.info("Database cleanup complete.")


class WebhookServer:
    def __init__(self, host="127.0.0.1", port=8000):
        self.host = host
        self.port = port
        self.app = FastAPI()
        self.received_webhooks: List[Dict[str, Any]] = []
        self.server_thread = None
        self.server = None
        self.response_delay = 0

        @self.app.post("/webhook")
        async def webhook_endpoint(request: Request):
            time.sleep(self.response_delay)
            payload = await request.json()
            self.received_webhooks.append(payload)
            return {"status": "success"}

        @self.app.get("/health")
        async def health_check():
            return {"status": "ok"}

    def start(self):
        from uvicorn import Config, Server

        def run_server():
            config = Config(self.app, host=self.host, port=self.port, log_level="error")
            self.server = Server(config)
            self.server.run()

        logger.info("Starting webhook server...")
        self.server_thread = threading.Thread(target=run_server, daemon=True)
        self.server_thread.start()
        self._wait_for_server_ready()
        logger.info("Webhook server is running.")

    def stop(self):
        if self.server:
            logger.info("Stopping webhook server...")
            self.server.should_exit = True
            self.server_thread.join()
            logger.info("Webhook server stopped.")

    def _wait_for_server_ready(self, timeout=10):
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = requests.get(f"http://{self.host}:{self.port}/health")
                if response.status_code == 200 and response.json().get("status") == "ok":
                    logger.info("Webhook server health check passed.")
                    return
            except requests.exceptions.ConnectionError:
                logger.debug("Waiting for webhook server to be ready...")
                time.sleep(0.1)
        raise RuntimeError("Webhook server failed to pass health check within the timeout.")


class BaseTestCase(unittest.TestCase):
    db = None
    webhook_server = None

    @classmethod
    def setUpClass(cls):
        logger.info("Setting up test resources...")
        cls.db = PostgresConnection()
        cls.db.connect()
        cls.db.setup_environment()

        cls.webhook_server = WebhookServer()
        cls.webhook_server.start()
        logger.info("Test resources set up successfully.")

    def setUp(self):
        logger.info("Resetting test environment...")
        self.db.cur.execute("TRUNCATE employees RESTART IDENTITY CASCADE;")
        self.webhook_server.received_webhooks.clear()
        logger.info("Test environment reset.")

    @classmethod
    def tearDownClass(cls):
        logger.info("Tearing down test resources...")
        if cls.db:
            cls.db.cleanup()
            cls.db.close()
        if cls.webhook_server:
            cls.webhook_server.stop()
        logger.info("Test resources torn down.")

    def _wait_for_webhook(self, timeout=5, start_index=0):
        """
        Waits for a new webhook, starting from the given index.

        :param timeout: Maximum time to wait for a webhook.
        :param start_index: The index to start checking for new webhooks.
        :return: The newly received webhook.
        :raises AssertionError: If no new webhook is received within the timeout.
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if len(self.webhook_server.received_webhooks) > start_index:
                return self.webhook_server.received_webhooks[start_index]
            time.sleep(0.1)
        raise AssertionError("Webhook not received within timeout")
