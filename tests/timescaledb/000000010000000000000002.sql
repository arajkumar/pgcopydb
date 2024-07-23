-- KEEPALIVE {"lsn":"0/23FB3C8","timestamp":"2024-07-23 07:03:00.522486+0000"}
BEGIN; -- {"xid":764,"lsn":"0/24F97E8","timestamp":"2024-07-23 07:03:00.531091+0000","commit_lsn":"0/250D090"}
PREPARE 38024536 AS INSERT INTO "public"."metrics" ("id", "time", "name", "value") overriding system value VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12);
EXECUTE 38024536["1","2024-07-23 04:44:51.466335+00","test-1","-1","2","2024-07-23 05:44:51.466335+00","test","0","3","2024-07-23 06:44:51.466335+00","test+1","1"];
COMMIT; -- {"xid":764,"lsn":"0/250D090","timestamp":"2024-07-23 07:03:00.531091+0000"}
BEGIN; -- {"xid":765,"lsn":"0/250D090","timestamp":"2024-07-23 07:03:00.533675+0000","commit_lsn":"0/251BB40"}
PREPARE 1d838ea7 AS INSERT INTO "Foo"."MetricsWithPrefix" ("id", "time", "name", "value") overriding system value VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12);
EXECUTE 1d838ea7["1","2024-07-23 04:44:51.466335+00","test-1","-1","2","2024-07-23 05:44:51.466335+00","test","0","3","2024-07-23 06:44:51.466335+00","test+1","1"];
COMMIT; -- {"xid":765,"lsn":"0/251BB40","timestamp":"2024-07-23 07:03:00.533675+0000"}
-- KEEPALIVE {"lsn":"0/2522C30","timestamp":"2024-07-23 07:03:00.537858+0000"}
-- ENDPOS {"lsn":"0/2522C30"}
