-- transaction that spans multiple WAL files

BEGIN;

INSERT INTO table_a(f1) VALUES (50011);
COMMIT;

BEGIN;
    -- generate 100000 rows for table_a
    SELECT pg_switch_wal();
    INSERT INTO table_a(f1) SELECT (random() * 100 + 1)::int FROM generate_series(1, 1000000);
    SELECT pg_switch_wal();
ROLLBACK;

BEGIN;

INSERT INTO table_a(f1) VALUES ((random() * 100 + 1)::int);

SELECT
    pg_switch_wal();

INSERT INTO table_a(f1) VALUES ((random() * 100 + 1)::int);

SELECT
    pg_switch_wal();

INSERT INTO table_a(f1) VALUES (10001001), (101), (10001002), (104), (10001003);

SELECT
    pg_switch_wal();

INSERT INTO table_a(f1) VALUES ((random() * 100 + 1)::int);

SELECT
    pg_switch_wal();

COMMIT;
