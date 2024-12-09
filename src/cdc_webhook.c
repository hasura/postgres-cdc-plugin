/**
 * @file cdc_webhook.c
 * @brief PostgreSQL Change Data Capture (CDC) Webhook Extension
 *
 * This extension enables real-time monitoring of database changes by sending webhook
 * notifications when specified tables are modified. It supports INSERT, UPDATE, and
 * DELETE operations with configurable retry mechanisms and custom HTTP headers.
 *
 * The implementation uses libcurl for HTTP requests and provides robust error
 * handling and retry logic with both linear and exponential backoff strategies.
 *
 * @note This extension requires libcurl development libraries to be installed.
 */

#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"
#include <curl/curl.h>
#include <unistd.h>
#include <math.h>

#include "cdc_webhook_worker.h"

/* PostgreSQL extension magic */
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Function declarations */
PG_FUNCTION_INFO_V1(call_webhook);

/**
 * @brief Structure to hold webhook configuration
 *
 * Groups related webhook parameters together for better organization
 * and potential future extensions.
 */
typedef struct {
    const char* url;             /* Destination URL for the webhook */
    int timeout;                 /* Request timeout in seconds */
    bool cancel_on_failure;      /* Whether to abort transaction on failure */
    int retry_count;            /* Maximum number of retry attempts */
    int retry_interval;         /* Base interval between retries in seconds */
    const char* retry_strategy; /* LINEAR or EXPONENTIAL backoff */
} WebhookConfig;

/**
 * @brief Adds HTTP headers from a JSONB object to a curl request
 *
 * Processes a JSONB object containing header key-value pairs and adds them
 * to the curl request headers list.
 *
 * @param curl CURL handle
 * @param headers Pointer to curl header list
 * @param jsonb_headers JSONB object containing header key-value pairs
 */
static void add_headers_from_jsonb(
    CURL *curl,
    struct curl_slist **headers,
    Jsonb *jsonb_headers
) {
    JsonbIterator *iterator;
    JsonbValue value;
    JsonbIteratorToken token;
    bool is_key = true;
    StringInfoData current_header;

    initStringInfo(&current_header);
    iterator = JsonbIteratorInit(&jsonb_headers->root);

    while ((token = JsonbIteratorNext(&iterator, &value, true)) != WJB_DONE) {
        if (token == WJB_KEY) {
            /* Start a new header with the key */
            resetStringInfo(&current_header);
            appendStringInfoString(&current_header,
                pnstrdup(value.val.string.val, value.val.string.len));
            appendStringInfoString(&current_header, ": ");
            is_key = false;
        }
        else if (token == WJB_VALUE && !is_key) {
            /* Add the value and append to headers list */
            if (value.type == jbvString) {
                appendStringInfoString(&current_header,
                    pnstrdup(value.val.string.val, value.val.string.len));
                *headers = curl_slist_append(*headers, current_header.data);
            }
            is_key = true;
        }
    }

    pfree(current_header.data);
}

/**
 * @brief Calculates the delay for the next retry attempt
 *
 * @param config Webhook configuration
 * @param attempt Current attempt number (0-based)
 * @return Number of seconds to wait before next attempt
 */
static int calculate_retry_delay(const WebhookConfig *config, int attempt) {
    if (strcmp(config->retry_strategy, "LINEAR") == 0) {
        return config->retry_interval;
    }
    /* EXPONENTIAL: interval * 2^attempt */
    return config->retry_interval * (1 << attempt);
}

/**
 * @brief Performs a single webhook call attempt
 *
 * @param curl Initialized CURL handle
 * @param payload JSON payload to send
 * @param headers HTTP headers list
 * @param config Webhook configuration
 * @param err_msg Buffer for error messages
 * @return true if successful, false otherwise
 */
static bool attempt_webhook_call(
    CURL *curl,
    const char *payload,
    struct curl_slist *headers,
    const WebhookConfig *config,
    StringInfo err_msg
) {
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_URL, config->url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, config->timeout);

    CURLcode res = curl_easy_perform(curl);
    if (res == CURLE_OK) {
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        if (http_code >= 200 && http_code < 300) {
            return true;
        }
        appendStringInfo(err_msg, "HTTP %ld. ", http_code);
    } else {
        appendStringInfo(err_msg, "CURL error: %s. ", curl_easy_strerror(res));
    }
    return false;
}

/**
 * @brief Main webhook function called by PostgreSQL
 *
 * Sends webhook notifications with retry logic based on configuration.
 * Handles all necessary cleanup and error reporting.
 */
Datum call_webhook(PG_FUNCTION_ARGS) {
    /* Extract function arguments */
    Jsonb *payload = PG_GETARG_JSONB_P(0);
    text *url_text = PG_GETARG_TEXT_PP(1);
    Jsonb *headers_jsonb = PG_GETARG_JSONB_P(2);

    /* Initialize webhook configuration */
    WebhookConfig config = {
        .url = text_to_cstring(url_text),
        .timeout = PG_GETARG_INT32(3),
        .cancel_on_failure = PG_GETARG_BOOL(4),
        .retry_count = PG_GETARG_INT32(5),
        .retry_interval = PG_GETARG_INT32(6),
        .retry_strategy = text_to_cstring(PG_GETARG_TEXT_PP(7))
    };

    int current_attempt = 0;
    bool success = false;
    StringInfoData err_msg;
    initStringInfo(&err_msg);

    /* Initialize libcurl */
    curl_global_init(CURL_GLOBAL_ALL);

    /* Attempt webhook delivery with retries */
    while (current_attempt <= config.retry_count && !success) {
        /* Handle retry delay if this isn't the first attempt */
        if (current_attempt > 0) {
            int delay = calculate_retry_delay(&config, current_attempt - 1);
            appendStringInfo(&err_msg, "Attempt %d/%d failed. ",
                           current_attempt, config.retry_count + 1);

            ereport(NOTICE,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                     errmsg("Retrying webhook call in %d seconds (attempt %d/%d)",
                            delay, current_attempt + 1, config.retry_count + 1)));

            sleep(delay);
        }

        /* Prepare and execute webhook call */
        CURL *curl = curl_easy_init();
        if (curl) {
            struct curl_slist *headers = NULL;
            headers = curl_slist_append(headers, "Content-Type: application/json");
            add_headers_from_jsonb(curl, &headers, headers_jsonb);

            /* Convert payload to string */
            StringInfoData payload_str;
            initStringInfo(&payload_str);
            appendStringInfo(&payload_str, "%s",
                JsonbToCString(NULL, &payload->root, VARSIZE(payload)));

            /* Attempt the webhook call */
            success = attempt_webhook_call(curl, payload_str.data, headers,
                &config, &err_msg);

            /* Cleanup */
            pfree(payload_str.data);
            curl_slist_free_all(headers);
            curl_easy_cleanup(curl);
        }

        current_attempt++;
    }

    /* Final cleanup */
    curl_global_cleanup();

    /* Handle failure cases */
    if (!success) {
        if (config.cancel_on_failure) {
            ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                     errmsg("Webhook delivery failed: %s", err_msg.data)));
        } else {
            ereport(WARNING,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                     errmsg("Webhook delivery failed: %s", err_msg.data)));
        }
    }

    pfree(err_msg.data);
    PG_RETURN_VOID();
}