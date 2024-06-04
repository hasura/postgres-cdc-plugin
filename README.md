This is an expirimental postgres CDC plugin that is written in C & SQL.

The purpose of this plugin is to call an external webhook with event data when an event occurs.

You can build this plugin using docker:

```docker build -t pg_cdc_webhook .```

Then run the database with the plugin using:

```docker run -it --rm --name pg_cdc_webhook -p 5432:5432 pg_cdc_webhook```

Then you can connect to the database in a new terminal using psql. 

```psql -h localhost -p 5432 -U postgres -d testdb```

From there create the Extension:

```sql
CREATE EXTENSION cdc_webhook;
```

Create a table:

```sql
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name TEXT,                                  
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Register the table to use webhook CDC.

```sql
SELECT new_webhook_table_trigger(
    'test_table',
    'http://host.docker.internal:8000/webhook/',
    ARRAY['INSERT', 'UPDATE', 'DELETE'], 'new_trigger',
    ARRAY['name'], 5, true);
```

The SQL function called has this signature: 

```sql
CREATE OR REPLACE FUNCTION new_webhook_table_trigger(
    table_name TEXT,
    webhook_url TEXT,
    operations TEXT[],
    trigger_name TEXT,
    update_columns TEXT[] DEFAULT '{}',
    timeout INT DEFAULT 10,  -- Default timeout is 10 seconds
    cancel_on_failure BOOLEAN DEFAULT false  -- Default is not to cancel on failure
)
```

* table_name: The name of the table to listen for CDC on
* webhook_url: The webhook to send a POST request to with the change data
* operations: An ARRAY[] of operations like ARRAY['INSERT', 'UPDATE', 'DELETE'] to listen on
* trigger_name: The name of the trigger
* update_columns: The columns to listen for updates on. If any of the columns have a different value, then the webhook will be called.
* timeout: The timeout for the curl request
* cancel_on_failure: A boolean that when set to true will cause the transaction to FAIL if the curl request fails or times out.

In a new terminal you can run the Python server after installing the requirements.

```pip3 install -r requirements.txt```

Then start the python server:

```uvicorn webhook:app --reload```

Now if you insert some data into the table:

```sql
INSERT INTO test_table (name) VALUES ('Test Name');
```

The API will receive a call to the webhook!

```json
{
    "id": "21eb8c8e-e37b-4898-9e4d-64ff11abccd2",
    "event": {
        "op": "INSERT",
        "data": {
            "new": {
                "id": 1,
                "name": "Test Name",
                "created_at": "2024-06-04T04:27:09.379581"
            },
            "old": null
        },
        "trace_context": {
            "span_id": "3ba18ae6228cba46",
            "trace_id": "a98cb1af8fece761"
        }
    },
    "table": {
        "name": "test_table",
        "schema": "public"
    },
    "trigger": {
        "name": "new_trigger"
    },
    "created_at": "2024-06-04T04:27:09.379581+00:00",
    "delivery_info": {
        "max_retries": 0,
        "current_retry": 0
    }
}
```

Thoughts on future work:

* Add Retries
* Add push to Kafka