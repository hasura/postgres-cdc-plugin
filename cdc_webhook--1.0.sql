CREATE OR REPLACE FUNCTION call_webhook(payload JSONB, webhook_url TEXT, timeout INT, cancel_on_failure BOOLEAN) RETURNS void LANGUAGE c AS '/usr/lib/postgresql/16/lib/cdc_webhook', 'call_webhook';

CREATE OR REPLACE FUNCTION new_webhook_table_trigger(
    table_name TEXT,
    webhook_url TEXT,
    operations TEXT[],
    trigger_name TEXT,
    update_columns TEXT[] DEFAULT '{}',
    timeout INT DEFAULT 10,  -- Default timeout is 10 seconds
    cancel_on_failure BOOLEAN DEFAULT false  -- Default is not to cancel on failure
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    function_name TEXT := table_name || '_cdc_function_' || trigger_name;
    operation_clause TEXT := '';
    column_checks TEXT := '';
BEGIN
    -- Build operation clause
    IF array_length(operations, 1) > 0 THEN
        operation_clause := 'AFTER ' || array_to_string(operations, ' OR ') || ' ON ';
    ELSE
        RAISE EXCEPTION 'Operations must be specified';
    END IF;

    -- Build column checks for UPDATE
    IF array_length(update_columns, 1) > 0 THEN
        FOR i IN 1 .. array_length(update_columns, 1) LOOP
            column_checks := column_checks || format(
                'IF (NEW.%I IS DISTINCT FROM OLD.%I) THEN columns_changed := TRUE; END IF; ',
                update_columns[i], update_columns[i]
            );
        END LOOP;
    END IF;

    EXECUTE format('
        CREATE OR REPLACE FUNCTION %I() RETURNS trigger LANGUAGE plpgsql AS
        $function$
        DECLARE
            payload JSONB;
            trace_id TEXT := substr(md5(random()::text), 1, 16);
            span_id TEXT := substr(md5(random()::text), 1, 16);
            columns_changed BOOLEAN := FALSE;
        BEGIN

            IF TG_OP = ''INSERT'' THEN
                payload := jsonb_build_object(
                    ''created_at'', current_timestamp,
                    ''delivery_info'', jsonb_build_object(
                        ''max_retries'', 0,
                        ''current_retry'', 0
                    ),
                    ''event'', jsonb_build_object(
                        ''op'', TG_OP,
                        ''data'', jsonb_build_object(
                            ''old'', NULL,
                            ''new'', row_to_json(NEW)
                        ),
                        ''trace_context'', jsonb_build_object(
                            ''trace_id'', trace_id,
                            ''span_id'', span_id
                        )
                    ),
                    ''id'', gen_random_uuid(),
                    ''table'', jsonb_build_object(
                        ''schema'', TG_TABLE_SCHEMA,
                        ''name'', TG_TABLE_NAME
                    ),
                    ''trigger'', jsonb_build_object(
                        ''name'', %L
                    )
                );
            ELSIF TG_OP = ''UPDATE'' THEN
                %s -- column_checks inserted here
                IF NOT columns_changed THEN
                    RAISE NOTICE ''No specified columns were changed'';
                    RETURN NULL;
                END IF;
                payload := jsonb_build_object(
                    ''created_at'', current_timestamp,
                    ''delivery_info'', jsonb_build_object(
                        ''max_retries'', 0,
                        ''current_retry'', 0
                    ),
                    ''event'', jsonb_build_object(
                        ''op'', TG_OP,
                        ''data'', jsonb_build_object(
                            ''old'', row_to_json(OLD),
                            ''new'', row_to_json(NEW)
                        ),
                        ''trace_context'', jsonb_build_object(
                            ''trace_id'', trace_id,
                            ''span_id'', span_id
                        )
                    ),
                    ''id'', gen_random_uuid(),
                    ''table'', jsonb_build_object(
                        ''schema'', TG_TABLE_SCHEMA,
                        ''name'', TG_TABLE_NAME
                    ),
                    ''trigger'', jsonb_build_object(
                        ''name'', %L
                    )
                );
            ELSIF TG_OP = ''DELETE'' THEN
                payload := jsonb_build_object(
                    ''created_at'', current_timestamp,
                    ''delivery_info'', jsonb_build_object(
                        ''max_retries'', 0,
                        ''current_retry'', 0
                    ),
                    ''event'', jsonb_build_object(
                        ''op'', TG_OP,
                        ''data'', jsonb_build_object(
                            ''old'', row_to_json(OLD),
                            ''new'', NULL
                        ),
                        ''trace_context'', jsonb_build_object(
                            ''trace_id'', trace_id,
                            ''span_id'', span_id
                        )
                    ),
                    ''id'', gen_random_uuid(),
                    ''table'', jsonb_build_object(
                        ''schema'', TG_TABLE_SCHEMA,
                        ''name'', TG_TABLE_NAME
                    ),
                    ''trigger'', jsonb_build_object(
                        ''name'', %L
                    )
                );
            END IF;
            PERFORM call_webhook(payload, %L, %s, %L);  -- Pass timeout and cancel_on_failure here
            RETURN NULL;
        END;
        $function$;
    ', function_name, trigger_name, column_checks, trigger_name, trigger_name, webhook_url, timeout, cancel_on_failure);

    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', trigger_name, table_name);
    EXECUTE format('
        CREATE TRIGGER %I
        %s %I
        FOR EACH ROW EXECUTE FUNCTION %I()
    ', trigger_name, operation_clause, table_name, function_name);
END;
$$;