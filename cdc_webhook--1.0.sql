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
LANGUAGE c AS 'cdc_webhook', 'call_webhook';

CREATE OR REPLACE FUNCTION create_event_trigger(
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
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    function_name TEXT := schema_name || '_' || table_name || '_cdc_function_' || trigger_name;
    operation_clause TEXT := '';
    column_checks TEXT := '';
BEGIN
    -- Validate inputs first
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

    IF array_length(operations, 1) > 0 THEN
        operation_clause := trigger_timing || ' ' || array_to_string(operations, ' OR ') || ' ON ';
    ELSE
        RAISE EXCEPTION 'Operations must be specified';
    END IF;

    IF array_length(update_columns, 1) > 0 THEN
        FOR i IN 1 .. array_length(update_columns, 1) LOOP
            column_checks := column_checks || format(
                'IF (NEW.%I IS DISTINCT FROM OLD.%I) THEN columns_changed := TRUE; END IF; ',
                update_columns[i], update_columns[i]
            );
        END LOOP;
    END IF;

    -- Create the trigger function
    EXECUTE format('
        CREATE OR REPLACE FUNCTION %I() RETURNS trigger LANGUAGE plpgsql AS $func$
        DECLARE
            payload JSONB;
            webhook_headers JSONB := %L;
            webhook_endpoint TEXT := %L;
            columns_changed BOOLEAN := FALSE;
        BEGIN
            IF TG_OP = ''INSERT'' THEN
                payload := jsonb_build_object(
                    ''created_at'', current_timestamp,
                    ''event'', jsonb_build_object(
                        ''op'', TG_OP,
                        ''data'', jsonb_build_object(
                            ''old'', NULL,
                            ''new'', row_to_json(NEW)
                        )
                    ),
                    ''id'', gen_random_uuid(),
                    ''table'', jsonb_build_object(
                        ''schema'', TG_TABLE_SCHEMA,
                        ''name'', TG_TABLE_NAME
                    ),
                    ''trigger'', jsonb_build_object(
                        ''name'', %L,
                        ''timing'', %L
                    )
                );
            ELSIF TG_OP = ''UPDATE'' THEN
                %s
                IF array_length(%L::text[], 1) > 0 AND NOT columns_changed THEN
                    RETURN NULL;
                END IF;
                payload := jsonb_build_object(
                    ''created_at'', current_timestamp,
                    ''event'', jsonb_build_object(
                        ''op'', TG_OP,
                        ''data'', jsonb_build_object(
                            ''old'', row_to_json(OLD),
                            ''new'', row_to_json(NEW)
                        )
                    ),
                    ''id'', gen_random_uuid(),
                    ''table'', jsonb_build_object(
                        ''schema'', TG_TABLE_SCHEMA,
                        ''name'', TG_TABLE_NAME
                    ),
                    ''trigger'', jsonb_build_object(
                        ''name'', %L,
                        ''timing'', %L
                    )
                );
            ELSIF TG_OP = ''DELETE'' THEN
                payload := jsonb_build_object(
                    ''created_at'', current_timestamp,
                    ''event'', jsonb_build_object(
                        ''op'', TG_OP,
                        ''data'', jsonb_build_object(
                            ''old'', row_to_json(OLD),
                            ''new'', NULL
                        )
                    ),
                    ''id'', gen_random_uuid(),
                    ''table'', jsonb_build_object(
                        ''schema'', TG_TABLE_SCHEMA,
                        ''name'', TG_TABLE_NAME
                    ),
                    ''trigger'', jsonb_build_object(
                        ''name'', %L,
                        ''timing'', %L
                    )
                );
            END IF;

            -- Call webhook without exposing headers in error context
            PERFORM call_webhook(
                payload,
                webhook_endpoint,
                webhook_headers,
                %L, %L, %L, %L, %L
            );

            RETURN NULL;
        END;
        $func$;',
        function_name,
        headers,
        webhook_url,
        trigger_name, trigger_timing,
        column_checks,
        update_columns,
        trigger_name, trigger_timing,
        trigger_name, trigger_timing,
        timeout, cancel_on_failure, retry_number, retry_interval, retry_backoff
    );

    -- Create the trigger
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I.%I',
        trigger_name, schema_name, table_name);

    EXECUTE format(
        'CREATE TRIGGER %I %s %I.%I FOR EACH ROW EXECUTE FUNCTION %I()',
        trigger_name, operation_clause, schema_name, table_name, function_name
    );
END;
$$;