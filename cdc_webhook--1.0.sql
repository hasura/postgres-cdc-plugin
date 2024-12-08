--
-- PostgreSQL CDC Webhook Extension SQL Functions
-- Version: 1.0
-- Description: Enables Change Data Capture by sending webhook notifications for database changes
--

--
-- External C function for making HTTP webhook calls
-- This function is implemented in cdc_webhook.c
--
CREATE OR REPLACE FUNCTION call_webhook(
    payload JSONB,               -- The webhook payload to send
    webhook_url TEXT,            -- Destination URL
    headers JSONB,               -- Custom HTTP headers for authentication/configuration
    timeout INT,                 -- Request timeout in seconds (how long to wait for response)
    cancel_on_failure BOOLEAN,   -- If true, rolls back transaction on webhook failure
    retry_number INT,            -- Maximum number of retry attempts (0 means no retries)
    retry_interval INT,          -- Base interval between retries in seconds
    retry_backoff TEXT           -- Retry strategy: 'LINEAR' (fixed intervals) or 'EXPONENTIAL' (increasing intervals)
) RETURNS void
LANGUAGE c AS 'cdc_webhook', 'call_webhook';

--
-- Main function to create CDC event triggers
-- Creates both the trigger function and the trigger itself
--
CREATE OR REPLACE FUNCTION create_event_trigger(
    table_name TEXT,                              -- Name of the table to monitor for changes
    webhook_url TEXT,                             -- URL where webhook notifications will be sent
    headers JSONB,                                -- HTTP headers for webhook requests (e.g., authentication tokens)
    operations TEXT[],                            -- Array of operations to monitor: 'INSERT', 'UPDATE', 'DELETE'
    trigger_name TEXT,                            -- Unique identifier for this trigger
    schema_name TEXT DEFAULT CURRENT_SCHEMA(),    -- Database schema containing the table
    update_columns TEXT[] DEFAULT '{}',           -- Specific columns to monitor for UPDATE operations
    timeout INT DEFAULT 10,                       -- Webhook request timeout in seconds
    cancel_on_failure BOOLEAN DEFAULT false,      -- Whether to roll back transaction on webhook failure
    trigger_timing TEXT DEFAULT 'AFTER',          -- When to fire trigger: 'BEFORE' or 'AFTER'
    retry_number INT DEFAULT 3,                   -- Number of retry attempts for failed webhook calls
    retry_interval INT DEFAULT 1,                 -- Base interval between retries in seconds
    retry_backoff TEXT DEFAULT 'LINEAR'           -- Retry strategy: 'LINEAR' or 'EXPONENTIAL'
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    -- Construct unique function name for this trigger
    function_name TEXT := schema_name || '_' || table_name || '_cdc_function_' || trigger_name;
    operation_clause TEXT;     -- Will hold the SQL for trigger operations
    column_checks TEXT;        -- Will hold the SQL for checking column changes
BEGIN
    --
    -- Input validation section
    --
    IF trigger_timing NOT IN ('BEFORE', 'AFTER') THEN
        RAISE EXCEPTION 'trigger_timing must be either BEFORE or AFTER';
    END IF;

    IF retry_backoff NOT IN ('LINEAR', 'EXPONENTIAL') THEN
        RAISE EXCEPTION 'retry_backoff must be either LINEAR or EXPONENTIAL';
    END IF;

    IF retry_number < 0 THEN
        RAISE EXCEPTION 'retry_number must be non-negative';
    END IF;

    IF retry_interval <= 0 THEN
        RAISE EXCEPTION 'retry_interval must be positive';
    END IF;

    -- Construct the operation clause for the trigger
    IF array_length(operations, 1) > 0 THEN
        operation_clause := trigger_timing || ' ' || array_to_string(operations, ' OR ') || ' ON ';
    ELSE
        RAISE EXCEPTION 'Operations must be specified';
    END IF;

    -- Build SQL for checking which columns have changed
    SELECT string_agg(
        format(
            'IF (NEW.%I IS DISTINCT FROM OLD.%I) THEN columns_changed := TRUE; END IF;',
            col, col
        ),
        ' '
    )
    INTO column_checks
    FROM unnest(update_columns) AS col;

    --
    -- Create the trigger function
    --
    EXECUTE format($create_function$
        CREATE OR REPLACE FUNCTION %I()
        RETURNS trigger
        LANGUAGE plpgsql AS $trigger_func$
        DECLARE
            payload JSONB;                      -- Will hold the webhook payload
            webhook_headers JSONB := %L;        -- Headers for webhook request
            webhook_endpoint TEXT := %L;        -- URL for webhook
            columns_changed BOOLEAN := FALSE;   -- Tracks if monitored columns changed
        BEGIN
            -- For UPDATE operations, check if monitored columns changed
            IF TG_OP = 'UPDATE' THEN
                %s  -- Column change detection logic

                -- Skip if no monitored columns changed
                IF array_length(%L::text[], 1) > 0 AND NOT columns_changed THEN
                    RETURN NULL;
                END IF;
            END IF;

            --
            -- Construct the webhook payload
            --
            payload := jsonb_build_object(
                'created_at', current_timestamp,
                'id', gen_random_uuid(),
                'table', jsonb_build_object(
                    'schema', TG_TABLE_SCHEMA,
                    'name', TG_TABLE_NAME
                ),
                'trigger', jsonb_build_object(
                    'name', %L,
                    'timing', %L
                ),
                'event', jsonb_build_object(
                    'op', TG_OP,
                    'data', CASE TG_OP
                        WHEN 'INSERT' THEN jsonb_build_object(
                            'old', NULL,
                            'new', row_to_json(NEW)
                        )
                        WHEN 'UPDATE' THEN jsonb_build_object(
                            'old', row_to_json(OLD),
                            'new', row_to_json(NEW)
                        )
                        WHEN 'DELETE' THEN jsonb_build_object(
                            'old', row_to_json(OLD),
                            'new', NULL
                        )
                    END
                )
            );

            -- Send webhook with retry logic
            PERFORM call_webhook(
                payload,          -- The constructed JSON payload
                webhook_endpoint, -- Destination URL for webhook
                webhook_headers,  -- HTTP headers for request
                %L,               -- Request timeout
                %L,               -- Whether to cancel on failure
                %L,               -- Number of retry attempts
                %L,               -- Base retry interval
                %L                -- Retry backoff strategy
            );

            RETURN NULL;
        END;
        $trigger_func$;
    $create_function$,
        function_name,      -- Name of the trigger function
        headers,            -- Webhook headers
        webhook_url,        -- Webhook URL
        column_checks,      -- SQL for column change detection
        update_columns,     -- Array of columns to monitor
        trigger_name,       -- Name of the trigger
        trigger_timing,     -- BEFORE or AFTER
        timeout,            -- Webhook timeout
        cancel_on_failure,  -- Whether to cancel on failure
        retry_number,       -- Number of retries
        retry_interval,     -- Retry interval
        retry_backoff       -- Retry strategy
    );

    --
    -- Create the actual trigger
    --
    EXECUTE format('
        DROP TRIGGER IF EXISTS %I ON %I.%I;
        CREATE TRIGGER %I
        %s %I.%I
        FOR EACH ROW
        EXECUTE FUNCTION %I();
    ',
        trigger_name, schema_name, table_name,     -- Drop existing trigger
        trigger_name,                              -- New trigger name
        operation_clause, schema_name, table_name, -- Trigger conditions and table
        function_name                              -- Function to execute
    );
END;
$$;