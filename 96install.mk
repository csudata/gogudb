MODULE_big = gogudb

EXTENSION = gogudb

EXTVERSION = 1.0

target_version = 9.6

DATA_built = $(EXTENSION)--$(EXTVERSION).sql

PGFILEDESC = "gogudb - database cluster of PostgreSQL"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifneq ($(MAJORVERSION), $(target_version))
$(error target pgsql version: $(target_version) is need)
endif
