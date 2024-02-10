---
--- pgcopydb test/cdc/ddl.sql
---
--- This file implements DDL changes in the pagila database.

begin;

CREATE TABLE table_a (id serial PRIMARY KEY, f1 int4, f2 text);
CREATE TABLE table_b (id serial PRIMARY KEY, f1 int4, f2 text[]);

CREATE TABLE IF NOT EXISTS update_test
(
    id bigint primary key,
    name text
);

CREATE TABLE IF NOT EXISTS generated_column_test
(
    id bigint primary key,
    name text,
    greet_hello text generated always as ('Hello ' || name) stored,
    greet_hi  text generated always as ('Hi ' || name) stored,
    email text
);

commit;
