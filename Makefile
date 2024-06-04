EXTENSION = cdc_webhook
MODULE_big = cdc_webhook
OBJS = src/cdc_webhook.o
DATA = cdc_webhook--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

SHLIB_LINK = -lcurl