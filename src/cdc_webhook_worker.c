#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/guc.h"

#include <unistd.h>
#include <time.h>
#include <stdlib.h> /* For rand() */

/* Signal handling variables for the background worker */
static volatile sig_atomic_t got_SIGHUP = false;
static volatile sig_atomic_t got_SIGTERM = false;

/* Signal handlers */
static void handle_sighup(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_SIGHUP = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

static void handle_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_SIGTERM = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/* Background worker main function */
void PGDLLEXPORT cdc_webhook_worker_main(Datum main_arg)
{
    pqsignal(SIGHUP, handle_sighup);
    pqsignal(SIGTERM, handle_sigterm);
    BackgroundWorkerUnblockSignals();

    elog(LOG, "CDC Webhook Background Worker started.");

    srand((unsigned) time(NULL)); // Seed random number generator

    while (!got_SIGTERM)
    {
        /* Simulate random failure */
        if (rand() % 10 == 0) /* Simulated 10% chance of failure */
        {
            elog(ERROR, "Simulated crash in CDC Webhook Worker.");
            proc_exit(1); /* Exit with error to trigger restart */
        }

        /* Process pending webhook events from `cdc_webhook.event_log` */
        elog(LOG, "Checking for pending events in cdc_webhook.event_log...");

        /*
         * Here, you would normally query and process entries where status = 'PENDING',
         * calling the `call_webhook` function as necessary.
         */

        /* Simulate task processing delay */
        pg_usleep(1 * 1000000L); // Sleep for 1 second

        /* Handle latch signals */
        ResetLatch(MyLatch);

        if (got_SIGHUP)
        {
            got_SIGHUP = false;
            ProcessConfigFile(PGC_SIGHUP);
            elog(LOG, "Configuration reloaded.");
        }
    }

    elog(LOG, "CDC Webhook Background Worker shutting down.");
    proc_exit(0);
}

void _PG_init(void)
{
    BackgroundWorker worker;

    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    snprintf(worker.bgw_name, BGW_MAXLEN, "CDC Webhook Background Worker");
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "cdc_webhook");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "cdc_webhook_worker_main");
    worker.bgw_restart_time = 1; /* Restart worker after 1 second */
    worker.bgw_main_arg = (Datum) 0;

    elog(LOG, "Registering CDC Webhook Background Worker...");
    RegisterBackgroundWorker(&worker);
    elog(LOG, "CDC Webhook Background Worker registered successfully.");
}
