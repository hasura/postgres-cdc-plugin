#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"
#include <curl/curl.h>
#include <unistd.h>
#include <math.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(call_webhook);

static void
add_headers_from_jsonb(CURL *curl, struct curl_slist **headers, Jsonb *jsonb_headers)
{
    JsonbIterator *it;
    JsonbValue v;
    JsonbIteratorToken type;
    bool is_key = true;
    StringInfoData current_header;

    initStringInfo(&current_header);

    it = JsonbIteratorInit(&jsonb_headers->root);

    while ((type = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
    {
        if (type == WJB_KEY)
        {
            resetStringInfo(&current_header);
            appendStringInfoString(&current_header, pnstrdup(v.val.string.val, v.val.string.len));
            appendStringInfoString(&current_header, ": ");
            is_key = false;
        }
        else if (type == WJB_VALUE && !is_key)
        {
            if (v.type == jbvString)
            {
                appendStringInfoString(&current_header, pnstrdup(v.val.string.val, v.val.string.len));
                *headers = curl_slist_append(*headers, current_header.data);
            }
            is_key = true;
        }
    }
}

Datum
call_webhook(PG_FUNCTION_ARGS)
{
    Jsonb *payload = PG_GETARG_JSONB_P(0);
    text *url_text = PG_GETARG_TEXT_PP(1);
    Jsonb *headers_jsonb = PG_GETARG_JSONB_P(2);
    int timeout = PG_GETARG_INT32(3);
    bool cancel_on_failure = PG_GETARG_BOOL(4);
    int retry_number = PG_GETARG_INT32(5);
    int retry_interval = PG_GETARG_INT32(6);
    text *retry_backoff_text = PG_GETARG_TEXT_PP(7);

    char *webhook_url = text_to_cstring(url_text);
    char *retry_backoff = text_to_cstring(retry_backoff_text);
    int current_attempt = 0;
    bool success = false;
    StringInfoData err_msg;

    initStringInfo(&err_msg);

    curl_global_init(CURL_GLOBAL_ALL);

    while (current_attempt <= retry_number && !success)
    {
        if (current_attempt > 0)
        {
            int delay;
            if (strcmp(retry_backoff, "LINEAR") == 0)
            {
                delay = retry_interval;
            }
            else // EXPONENTIAL
            {
                delay = retry_interval * (1 << (current_attempt - 1));
            }

            appendStringInfo(&err_msg, "Attempt %d/%d failed. ",
                           current_attempt, retry_number + 1);

            ereport(NOTICE,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                     errmsg("Retrying webhook call in %d seconds (attempt %d/%d)",
                            delay, current_attempt + 1, retry_number + 1)));

            sleep(delay);
        }

        CURL *curl = curl_easy_init();
        if(curl) {
            struct curl_slist *headers = NULL;
            headers = curl_slist_append(headers, "Content-Type: application/json");
            add_headers_from_jsonb(curl, &headers, headers_jsonb);

            StringInfoData buf;
            initStringInfo(&buf);
            appendStringInfo(&buf, "%s", JsonbToCString(NULL, &payload->root, VARSIZE(payload)));

            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
            curl_easy_setopt(curl, CURLOPT_URL, webhook_url);
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buf.data);
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout);

            CURLcode res = curl_easy_perform(curl);
            if(res == CURLE_OK) {
                long http_code = 0;
                curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
                if (http_code >= 200 && http_code < 300) {
                    success = true;
                } else {
                    appendStringInfo(&err_msg, "HTTP %ld. ", http_code);
                }
            } else {
                appendStringInfo(&err_msg, "CURL error: %s. ", curl_easy_strerror(res));
            }

            curl_slist_free_all(headers);
            curl_easy_cleanup(curl);
            pfree(buf.data);
        }

        current_attempt++;
    }

    curl_global_cleanup();

    if (!success) {
        if (cancel_on_failure) {
            ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                     errmsg("Webhook delivery failed: %s", err_msg.data)));
        } else {
            ereport(WARNING,
                    (errcode(ERRCODE_CONNECTION_FAILURE),
                     errmsg("Webhook delivery failed: %s", err_msg.data)));
        }
    }

    PG_RETURN_VOID();
}