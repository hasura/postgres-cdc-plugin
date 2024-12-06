# PostgreSQL CDC Webhook Extension

A PostgreSQL extension that enables Change Data Capture (CDC) by sending webhook notifications for database changes. This extension is written in C and SQL, allowing real-time monitoring of INSERT, UPDATE, and DELETE operations on specified tables.

## Features

- Real-time notifications via webhooks for database changes
- Support for multiple operation types (INSERT, UPDATE, DELETE)
- Configurable retry mechanisms with linear or exponential backoff
- Custom HTTP headers support for webhook authentication
- Flexible column tracking for UPDATE operations
- Support for both schema-qualified and public schema tables
- Configurable trigger timing (BEFORE or AFTER)
- Transaction control based on webhook delivery success
- Secure logging that strips sensitive header values

## Installation

### Using Docker

1. Build the Docker image:
```bash
docker build -t pg_cdc_webhook .
```

2. Run the container:
```bash
docker run -it --rm --name pg_cdc_webhook -p 5432:5432 pg_cdc_webhook
```

3. Connect to the database:
```bash
psql -h localhost -p 5432 -U postgres -d testdb
```

### Manual Installation

Requires:
- PostgreSQL development headers
- libcurl development libraries
- Build tools (make, gcc)
- PostgreSQL server

## Setup

1. Create the extension in your database:
```sql
CREATE EXTENSION cdc_webhook;
```

2. Create tables you want to monitor:
```sql
-- Example table in public schema
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name TEXT,
    salary INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Example table in custom schema
CREATE SCHEMA hr;
CREATE TABLE hr.employees (
    id SERIAL PRIMARY KEY,
    name TEXT,
    salary INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Usage

### Function: create_event_trigger

Creates a trigger that sends webhook notifications for specified table operations.

```sql
SELECT create_event_trigger(
    table_name TEXT,
    webhook_url TEXT,
    headers JSONB,
    operations TEXT[],
    trigger_name TEXT,
    schema_name TEXT DEFAULT CURRENT_SCHEMA(),
    update_columns TEXT[] DEFAULT '{}',
    timeout INT DEFAULT 10,
    cancel_on_failure BOOLEAN DEFAULT false,
    trigger_timing TEXT DEFAULT 'AFTER',
    retry_number INT DEFAULT 3,
    retry_interval INT DEFAULT 1,
    retry_backoff TEXT DEFAULT 'LINEAR'
);
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| table_name | TEXT | required | Name of the table to monitor |
| webhook_url | TEXT | required | URL to send webhook notifications to |
| headers | JSONB | required | HTTP headers for webhook requests (e.g., authentication) |
| operations | TEXT[] | required | Array of operations to monitor: 'INSERT', 'UPDATE', 'DELETE' |
| trigger_name | TEXT | required | Unique name for the trigger |
| schema_name | TEXT | CURRENT_SCHEMA() | Schema containing the table |
| update_columns | TEXT[] | '{}' | Columns to monitor for UPDATE operations. Empty array means NO columns will be tracked |
| timeout | INT | 10 | Webhook request timeout in seconds |
| cancel_on_failure | BOOLEAN | false | Whether to cancel the transaction if webhook delivery fails |
| trigger_timing | TEXT | 'AFTER' | When to fire the trigger: 'BEFORE' or 'AFTER' |
| retry_number | INT | 3 | Number of retry attempts for failed webhook calls |
| retry_interval | INT | 1 | Base interval between retries in seconds |
| retry_backoff | TEXT | 'LINEAR' | Retry backoff strategy: 'LINEAR' or 'EXPONENTIAL' |

#### Important Notes

1. **update_columns Behavior**:
   - Empty array ('{}') means NO columns will be tracked for updates
   - To track ALL columns, you must explicitly list all column names
   - Only listed columns will trigger webhook notifications when changed

2. **Retry Mechanism**:
   - LINEAR backoff: Waits `retry_interval` seconds between each attempt
   - EXPONENTIAL backoff: Waits `retry_interval * 2^n` seconds between attempts (n starts at 0)
   - Example with retry_interval=2:
     - LINEAR: 2s, 2s, 2s
     - EXPONENTIAL: 2s, 4s, 8s

3. **Transaction Behavior**:
   - When cancel_on_failure=true, transaction will roll back if webhook delivery fails
   - Failures are logged as warnings when cancel_on_failure=false

4. **Log Security**:
   - Header values are not exposed in PostgreSQL logs or error messages
   - Instead of showing actual header values, logs will reference them as 'webhook_headers'
   - Example log message:
     ```
     NOTICE:  Retrying webhook call in 2 seconds (attempt 2/4)
     ERROR:  Webhook delivery failed: Attempt 1/3 failed. HTTP 401.
     CONTEXT:  SQL statement "SELECT call_webhook(payload, webhook_endpoint, webhook_headers, 5, 't', 3, 2, 'EXPONENTIAL')"
     ```

### Example Usage

#### Basic Setup (Public Schema)
```sql
SELECT create_event_trigger(
    'employees',                                      -- table name
    'http://host.docker.internal:8000/webhook/',      -- webhook URL
    '{"X-API-Key": "your-secret-key-here"}'::jsonb,  -- headers
    ARRAY['INSERT', 'UPDATE', 'DELETE'],             -- operations
    'employee_changes'                               -- trigger name
);
```

#### Advanced Setup (Custom Schema)
```sql
SELECT create_event_trigger(
    'employees',                                          -- table name
    'http://host.docker.internal:8000/webhook/',          -- webhook URL
    '{"X-API-Key": "your-secret-key-here"}'::jsonb,      -- headers
    ARRAY['INSERT', 'UPDATE', 'DELETE'],                 -- operations
    'hr_employee_changes',                               -- trigger name
    'hr',                                               -- schema name
    ARRAY['salary', 'name'],                            -- track only salary and name changes
    5,                                                  -- 5 second timeout
    true,                                               -- cancel transaction on webhook failure
    'AFTER',                                            -- trigger timing
    3,                                                  -- 3 retries
    2,                                                  -- 2 second base interval
    'EXPONENTIAL'                                       -- exponential backoff
);
```

## Webhook Payload Format

The webhook receives a JSON payload with the following structure:

```json
{
    "id": "21eb8c8e-e37b-4898-9e4d-64ff11abccd2",
    "event": {
        "op": "UPDATE",
        "data": {
            "new": {
                "id": 1,
                "name": "John Doe",
                "salary": 80000,
                "created_at": "2024-06-04T04:27:09.379581"
            },
            "old": {
                "id": 1,
                "name": "John Doe",
                "salary": 75000,
                "created_at": "2024-06-04T04:27:09.379581"
            }
        }
    },
    "table": {
        "name": "employees",
        "schema": "public"
    },
    "trigger": {
        "name": "employee_changes",
        "timing": "AFTER"
    },
    "created_at": "2024-06-04T04:27:09.379581+00:00"
}
```

## Testing Webhook Integration

1. Install Python dependencies:
```bash
pip3 install -r requirements.txt
```

2. Start the webhook server:
```bash
uvicorn webhook:app --reload
```

The server will log all received webhooks to the console for debugging.

## Testing with Sample Data

After setting up the triggers, you can test the webhook notifications with various operations:

### Insert Operations
```sql
-- Insert into public schema
INSERT INTO employees (name, salary) 
VALUES 
    ('John Doe', 75000),
    ('Jane Smith', 82000),
    ('Bob Wilson', 65000);

-- Insert into hr schema
INSERT INTO hr.employees (name, salary) 
VALUES 
    ('Alice Brown', 90000),
    ('Charlie Davis', 78000);
```

### Update Operations
```sql
-- Update single record
UPDATE employees 
SET salary = 80000 
WHERE name = 'John Doe';

-- Bulk update
UPDATE hr.employees 
SET salary = salary * 1.1 
WHERE salary < 80000;
```

### Delete Operations
```sql
-- Delete single record
DELETE FROM employees 
WHERE name = 'Bob Wilson';

-- Conditional delete
DELETE FROM hr.employees 
WHERE salary < 75000;
```

## Development Notes

- The extension uses libcurl for HTTP requests
- All webhook calls are synchronous
- Failed webhook calls are retried based on configuration
- Webhook timeouts are handled at the PostgreSQL level
- Sensitive header values are stripped from logs
- Error messages provide useful information without exposing secrets

## Limitations

- Webhook calls are synchronous and may impact transaction performance
- No built-in webhook payload encryption
- No built-in webhook authentication beyond custom headers