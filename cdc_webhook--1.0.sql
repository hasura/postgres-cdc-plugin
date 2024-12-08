--==============================================================================
-- PostgreSQL CDC Webhook Extension SQL Functions
-- Version: 1.0
--
-- Description:
--     This extension provides mechanisms for Change Data Capture (CDC) using
--     PostgreSQL triggers. When configured, it sends webhook notifications
--     (HTTP requests) upon insert, update, or delete operations on specified tables.
--
-- Features:
--     - Supports both "NONE" and "PRIVATE" security modes for storing sensitive
--       webhook credentials.
--     - Allows specifying HTTP headers, retry strategies, and request timeouts.
--     - Offers row-level security for stored credentials.
--
-- Note:
--     The `call_webhook()` function is assumed to be implemented in C and provided
--     by an external extension (compiled and installed C library).
--==============================================================================

CREATE SCHEMA IF NOT EXISTS cdc_webhook;
REVOKE ALL ON SCHEMA cdc_webhook FROM PUBLIC;


CREATE TABLE IF NOT EXISTS cdc_webhook.credentials
(
    id             SERIAL PRIMARY KEY,
    trigger_schema TEXT  NOT NULL,
    trigger_table  TEXT  NOT NULL,
    trigger_name   TEXT  NOT NULL,
    webhook_url    TEXT  NOT NULL,
    headers        JSONB NOT NULL,
    created_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    created_by     NAME        DEFAULT CURRENT_USER,
    UNIQUE (trigger_schema, trigger_table, trigger_name)
);

-- Enable row-level security
ALTER TABLE cdc_webhook.credentials
    ENABLE ROW LEVEL SECURITY;

-- Policy:
-- Only superusers (or a specified role 'postgres') can access these rows.
-- Adjust pg_has_role check as needed for your environment.
CREATE POLICY credentials_access ON cdc_webhook.credentials
    FOR ALL
    TO PUBLIC
    USING (pg_has_role(CURRENT_USER, 'postgres', 'MEMBER'));


CREATE OR REPLACE FUNCTION cdc_webhook.credentials_audit_trigger()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.updated_at := CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER credentials_audit
    BEFORE UPDATE
    ON cdc_webhook.credentials
    FOR EACH ROW
EXECUTE FUNCTION cdc_webhook.credentials_audit_trigger();



CREATE OR REPLACE FUNCTION call_webhook(
    payload JSONB,
    webhook_url TEXT,
    headers JSONB,
    timeout INT,
    cancel_on_failure BOOLEAN,
    retry_number INT,
    retry_interval INT,
    retry_backoff TEXT
) RETURNS void
    LANGUAGE c AS
'cdc_webhook',
'call_webhook';



CREATE OR REPLACE FUNCTION create_event_trigger(
    table_name TEXT,
    webhook_url TEXT,
    headers JSONB,
    operations TEXT[],
    name TEXT,
    schema_name TEXT DEFAULT CURRENT_SCHEMA(),
    update_columns TEXT[] DEFAULT '{}',
    timeout INT DEFAULT 10,
    cancel_on_failure BOOLEAN DEFAULT false,
    trigger_timing TEXT DEFAULT 'AFTER',
    retry_number INT DEFAULT 3,
    retry_interval INT DEFAULT 1,
    retry_backoff TEXT DEFAULT 'LINEAR',
    security TEXT DEFAULT 'NONE'
) RETURNS void
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path = pg_catalog, pg_temp
AS
$$
DECLARE
    function_name      TEXT := 'cdc_' || gen_random_uuid()::text;
    operation_clause   TEXT;
    column_checks      TEXT;
    stored_webhook_url TEXT;
    stored_headers     JSONB;
BEGIN
    -- Validate trigger_timing
    IF trigger_timing NOT IN ('BEFORE', 'AFTER') THEN
        RAISE EXCEPTION 'trigger_timing must be either BEFORE or AFTER';
    END IF;

    -- Validate retry_backoff
    IF retry_backoff NOT IN ('LINEAR', 'EXPONENTIAL') THEN
        RAISE EXCEPTION 'retry_backoff must be either LINEAR or EXPONENTIAL';
    END IF;

    -- Validate security mode
    IF security NOT IN ('NONE', 'PRIVATE') THEN
        RAISE EXCEPTION 'security must be either NONE or PRIVATE';
    END IF;

    -- Validate retry_number and retry_interval
    IF retry_number < 0 THEN
        RAISE EXCEPTION 'retry_number must be non-negative';
    END IF;

    IF retry_interval <= 0 THEN
        RAISE EXCEPTION 'retry_interval must be positive';
    END IF;

    -- Validate operations
    IF array_length(operations, 1) IS NULL THEN
        RAISE EXCEPTION 'Operations must be specified and not empty';
    END IF;

    -- Set up security mode handling
    CASE security
        WHEN 'NONE' THEN stored_webhook_url := webhook_url;
                         stored_headers := headers;
        WHEN 'PRIVATE' THEN -- Insert or update credentials in the credentials table
        INSERT INTO cdc_webhook.credentials AS c ("trigger_schema", "trigger_table", "trigger_name", "webhook_url",
                                                  "headers")
        VALUES (schema_name, table_name, name, webhook_url, headers)
        ON CONFLICT ("trigger_schema", "trigger_table", "trigger_name")
            DO UPDATE SET "webhook_url" = EXCLUDED."webhook_url",
                          "headers"     = EXCLUDED."headers";

        stored_webhook_url := NULL; -- Will be retrieved at runtime from credentials
        stored_headers := NULL; -- Will be retrieved at runtime from credentials
        END CASE;

    -- Construct the trigger operation clause (e.g., "AFTER INSERT OR UPDATE OR DELETE ON")
    operation_clause := trigger_timing || ' ' || array_to_string(operations, ' OR ') || ' ON ';

    -- Generate column check statements for UPDATE operations if update_columns are specified
    SELECT string_agg(
                   format(
                           'IF (NEW.%I IS DISTINCT FROM OLD.%I) THEN columns_changed := TRUE; END IF;',
                           col, col
                       ),
                   ' '
               )
    INTO column_checks
    FROM unnest(update_columns) AS col;

    -- Create the dynamic trigger function
    EXECUTE format($create_function$
        CREATE OR REPLACE FUNCTION %I()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, pg_temp
        AS $trigger_func$
        DECLARE
            payload          JSONB;
            webhook_headers  JSONB;
            webhook_endpoint TEXT;
            columns_changed  BOOLEAN := FALSE;
        BEGIN
            -- If this is an UPDATE, check if any specified columns changed
            IF TG_OP = 'UPDATE' THEN
                %s
                IF array_length(%L::text[], 1) > 0 AND NOT columns_changed THEN
                    -- If no monitored columns changed, don't fire the webhook
                    RETURN NULL;
                END IF;
            END IF;

            -- Retrieve credentials based on the security mode
            CASE %L
                WHEN 'NONE' THEN
                    webhook_endpoint := %L;
                    webhook_headers := %L;
                WHEN 'PRIVATE' THEN
                    SELECT webhook_url, headers
                    INTO webhook_endpoint, webhook_headers
                    FROM cdc_webhook.credentials
                    WHERE trigger_schema = TG_TABLE_SCHEMA
                    AND trigger_table = TG_TABLE_NAME
                    AND trigger_name = %L;
            END CASE;

            -- Build the JSON payload describing the event
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

            -- Call the external webhook function
            PERFORM public.call_webhook(
                payload,
                webhook_endpoint,
                webhook_headers,
                %L,  -- timeout
                %L,  -- cancel_on_failure
                %L,  -- retry_number
                %L,  -- retry_interval
                %L   -- retry_backoff
            );

            RETURN NULL;
        END;
        $trigger_func$;
    $create_function$,
                   function_name,
                   column_checks,
                   update_columns,
                   security,
                   stored_webhook_url,
                   stored_headers,
                   name,
                   name,
                   trigger_timing,
                   timeout,
                   cancel_on_failure,
                   retry_number,
                   retry_interval,
                   retry_backoff
        );

    -- Drop existing trigger with the same name, if any, then create a new one
    EXECUTE format('
        DROP TRIGGER IF EXISTS %I ON %I.%I;
        CREATE TRIGGER %I
        %s %I.%I
        FOR EACH ROW
        EXECUTE FUNCTION %I();
    ',
                   name, schema_name, table_name,
                   name,
                   operation_clause, schema_name, table_name,
                   function_name
        );
END;
$$;


GRANT USAGE ON SCHEMA cdc_webhook TO PUBLIC;
GRANT EXECUTE ON FUNCTION create_event_trigger TO PUBLIC;
