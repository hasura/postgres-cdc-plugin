#ifndef CDC_WEBHOOK_WORKER_H
#define CDC_WEBHOOK_WORKER_H

#include "postgres.h"

/* Main function for the background worker */
extern void cdc_webhook_worker_main(Datum main_arg);

#endif /* CDC_WEBHOOK_WORKER_H */
