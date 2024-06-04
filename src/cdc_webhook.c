#include "postgres.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"  // Include this header for text_to_cstring
#include <curl/curl.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(call_webhook);

Datum call_webhook(PG_FUNCTION_ARGS);

Datum
call_webhook(PG_FUNCTION_ARGS)
{
    Jsonb *payload = PG_GETARG_JSONB_P(0);
    text *url_text = PG_GETARG_TEXT_PP(1);
    int timeout = PG_GETARG_INT32(2); // Get the timeout argument
    bool cancel_on_failure = PG_GETARG_BOOL(3); // Get the cancel_on_failure argument
    char *webhook_url = text_to_cstring(url_text);

    StringInfoData buf;
    initStringInfo(&buf);

    appendStringInfo(&buf, "%s", JsonbToCString(NULL, &payload->root, VARSIZE(payload)));

    CURL *curl;
    CURLcode res;

    curl_global_init(CURL_GLOBAL_ALL);
    curl = curl_easy_init();
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, webhook_url);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buf.data);
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout); // Set the timeout

        res = curl_easy_perform(curl);
        if(res != CURLE_OK) {
            if (cancel_on_failure) {
                ereport(ERROR,
                        (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                         errmsg("curl_easy_perform() failed: %s", curl_easy_strerror(res))));
            } else {
                elog(WARNING, "curl_easy_perform() failed: %s", curl_easy_strerror(res));  // Log the error
            }
        }

        curl_easy_cleanup(curl);
    } else {
        if (cancel_on_failure) {
            ereport(ERROR,
                    (errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                     errmsg("curl_easy_init() failed")));
        } else {
            elog(WARNING, "curl_easy_init() failed");  // Log the error
        }
    }
    curl_global_cleanup();

    PG_RETURN_VOID();
}
