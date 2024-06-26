begin;

create table if not exists "metrics" (
    id bigint,
    "time" timestamp with time zone not null,
    name text not null,
    value numeric not null
);

commit;
