MODULES = hba_fdw

REGRESS = hba_fdw

EXTENSION = hba_fdw
DATA = hba_fdw--1.0.sql
PGFILEDESC = "hba_fdw - PostgreSQL Foreign Data Wrapper to manipulate pg_hba.conf file"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
