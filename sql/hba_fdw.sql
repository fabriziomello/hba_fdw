\set VERBOSITY terse
CREATE EXTENSION hba_fdw;
CREATE SERVER hba FOREIGN DATA WRAPPER hba_fdw;
CREATE FOREIGN TABLE pg_hba (host TEXT) SERVER hba;
SET client_min_messages TO DEBUG1;
SELECT * FROM pg_hba;
SELECT count(1) FROM pg_catalog.pg_statistic WHERE starelid = 'pg_hba'::regclass;
ANALYZE pg_hba;
SELECT count(1) FROM pg_catalog.pg_statistic WHERE starelid = 'pg_hba'::regclass;
EXPLAIN (COSTS OFF, VERBOSE) SELECT * FROM pg_hba;
