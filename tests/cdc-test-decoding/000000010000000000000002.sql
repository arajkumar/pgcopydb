BEGIN; -- {"xid":493,"lsn":"0/24E4E90","timestamp":"2024-07-16 09:25:01.612948+0000","commit_lsn":"0/24E52F8"}
PREPARE d003ca15 AS INSERT INTO public.rental (rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update) overriding system value VALUES ($1, $2, $3, $4, $5, $6, $7);
EXECUTE d003ca15["16050","2022-06-01 00:00:00+00","371","291",null,"1","2022-06-01 00:00:00+00"];
PREPARE eba75101 AS INSERT INTO public.payment_p2022_06 (payment_id, customer_id, staff_id, rental_id, amount, payment_date) overriding system value VALUES ($1, $2, $3, $4, $5, $6);
EXECUTE eba75101["32099","291","1","16050","5.99","2022-06-01 00:00:00+00"];
COMMIT; -- {"xid":493,"lsn":"0/24E52F8","timestamp":"2024-07-16 09:25:01.612948+0000"}
BEGIN; -- {"xid":494,"lsn":"0/24E52F8","timestamp":"2024-07-16 09:25:01.613688+0000","commit_lsn":"0/24E63E0"}
PREPARE b44633db AS UPDATE public.payment_p2022_02 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE b44633db["11.95","23757","116","2","14763","11.99","2022-02-11 03:52:25.634006+00"];
PREPARE b44633db AS UPDATE public.payment_p2022_02 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE b44633db["11.95","24866","237","2","11479","11.99","2022-02-07 18:37:34.579143+00"];
PREPARE a7ce2dc4 AS UPDATE public.payment_p2022_03 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE a7ce2dc4["11.95","17055","196","2","106","11.99","2022-03-18 18:50:39.243747+00"];
PREPARE a7ce2dc4 AS UPDATE public.payment_p2022_03 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE a7ce2dc4["11.95","28799","591","2","4383","11.99","2022-03-08 16:41:23.911522+00"];
PREPARE a368a817 AS UPDATE public.payment_p2022_04 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE a368a817["11.95","20403","362","1","14759","11.99","2022-04-16 04:35:36.904758+00"];
PREPARE f53be34c AS UPDATE public.payment_p2022_05 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE f53be34c["11.95","17354","305","1","2166","11.99","2022-05-12 11:28:17.949049+00"];
PREPARE e6404448 AS UPDATE public.payment_p2022_06 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE e6404448["11.95","22650","204","2","15415","11.99","2022-06-11 11:17:22.428079+00"];
PREPARE e6404448 AS UPDATE public.payment_p2022_06 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE e6404448["11.95","24553","195","2","16040","11.99","2022-06-15 02:21:00.279776+00"];
PREPARE 4b3d4a5b AS UPDATE public.payment_p2022_07 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE 4b3d4a5b["11.95","28814","592","1","3973","11.99","2022-07-06 12:15:38.928947+00"];
PREPARE 4b3d4a5b AS UPDATE public.payment_p2022_07 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE 4b3d4a5b["11.95","29136","13","2","8831","11.99","2022-07-22 16:15:40.797771+00"];
COMMIT; -- {"xid":494,"lsn":"0/24E63E0","timestamp":"2024-07-16 09:25:01.613688+0000"}
BEGIN; -- {"xid":495,"lsn":"0/24E65A0","timestamp":"2024-07-16 09:25:01.613762+0000","commit_lsn":"0/24E66B0"}
PREPARE e1d51ac7 AS DELETE FROM public.payment_p2022_06 WHERE payment_id = $1 and customer_id = $2 and staff_id = $3 and rental_id = $4 and amount = $5 and payment_date = $6;
EXECUTE e1d51ac7["32099","291","1","16050","5.99","2022-06-01 00:00:00+00"];
PREPARE 3f2797d9 AS DELETE FROM public.rental WHERE rental_id = $1;
EXECUTE 3f2797d9["16050"];
COMMIT; -- {"xid":495,"lsn":"0/24E66B0","timestamp":"2024-07-16 09:25:01.613762+0000"}
BEGIN; -- {"xid":496,"lsn":"0/24E66B0","timestamp":"2024-07-16 09:25:01.613945+0000","commit_lsn":"0/24E6C30"}
PREPARE b44633db AS UPDATE public.payment_p2022_02 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE b44633db["11.99","23757","116","2","14763","11.95","2022-02-11 03:52:25.634006+00"];
PREPARE b44633db AS UPDATE public.payment_p2022_02 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE b44633db["11.99","24866","237","2","11479","11.95","2022-02-07 18:37:34.579143+00"];
PREPARE a7ce2dc4 AS UPDATE public.payment_p2022_03 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE a7ce2dc4["11.99","17055","196","2","106","11.95","2022-03-18 18:50:39.243747+00"];
PREPARE a7ce2dc4 AS UPDATE public.payment_p2022_03 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE a7ce2dc4["11.99","28799","591","2","4383","11.95","2022-03-08 16:41:23.911522+00"];
PREPARE a368a817 AS UPDATE public.payment_p2022_04 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE a368a817["11.99","20403","362","1","14759","11.95","2022-04-16 04:35:36.904758+00"];
PREPARE f53be34c AS UPDATE public.payment_p2022_05 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE f53be34c["11.99","17354","305","1","2166","11.95","2022-05-12 11:28:17.949049+00"];
PREPARE e6404448 AS UPDATE public.payment_p2022_06 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE e6404448["11.99","22650","204","2","15415","11.95","2022-06-11 11:17:22.428079+00"];
PREPARE e6404448 AS UPDATE public.payment_p2022_06 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE e6404448["11.99","24553","195","2","16040","11.95","2022-06-15 02:21:00.279776+00"];
PREPARE 4b3d4a5b AS UPDATE public.payment_p2022_07 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE 4b3d4a5b["11.99","28814","592","1","3973","11.95","2022-07-06 12:15:38.928947+00"];
PREPARE 4b3d4a5b AS UPDATE public.payment_p2022_07 SET amount = $1 WHERE payment_id = $2 and customer_id = $3 and staff_id = $4 and rental_id = $5 and amount = $6 and payment_date = $7;
EXECUTE 4b3d4a5b["11.99","29136","13","2","8831","11.95","2022-07-22 16:15:40.797771+00"];
COMMIT; -- {"xid":496,"lsn":"0/24E6C30","timestamp":"2024-07-16 09:25:01.613945+0000"}
BEGIN; -- {"xid":497,"lsn":"0/24E6C30","timestamp":"2024-07-16 09:25:01.614185+0000","commit_lsn":"0/24E6DE0"}
PREPARE 65b94c7d AS UPDATE public.staff SET first_name = $1, last_name = $2, address_id = $3, email = $4, store_id = $5, active = $6, username = $7, password = $8, last_update = $9, picture = $10 WHERE staff_id = $11;
EXECUTE 65b94c7d["Warner","Hudson","45","hartmann1448@ratkehaley.com","25","true","fay.kub","8cb2237d0679ca88db6464eac60da96345513964","2024-07-16 09:25:01.435038+00",null,"1"];
COMMIT; -- {"xid":497,"lsn":"0/24E6DE0","timestamp":"2024-07-16 09:25:01.614185+0000"}
BEGIN; -- {"xid":498,"lsn":"0/24E6DE0","timestamp":"2024-07-16 09:25:01.614280+0000","commit_lsn":"0/24E6EB8"}
PREPARE 4835081e AS INSERT INTO public."""dqname""" (id) overriding system value VALUES ($1);
EXECUTE 4835081e["1"];
COMMIT; -- {"xid":498,"lsn":"0/24E6EB8","timestamp":"2024-07-16 09:25:01.614280+0000"}
BEGIN; -- {"xid":499,"lsn":"0/24E6EB8","timestamp":"2024-07-16 09:25:01.614362+0000","commit_lsn":"0/24E6FD8"}
PREPARE 7a201c42 AS INSERT INTO public.identifer_as_column ("time") overriding system value VALUES ($1);
EXECUTE 7a201c42["1"];
PREPARE df296f92 AS DELETE FROM public.identifer_as_column WHERE "time" = $1;
EXECUTE df296f92["1"];
COMMIT; -- {"xid":499,"lsn":"0/24E6FD8","timestamp":"2024-07-16 09:25:01.614362+0000"}
BEGIN; -- {"xid":500,"lsn":"0/24E6FD8","timestamp":"2024-07-16 09:25:01.614482+0000","commit_lsn":"0/24E7090"}
PREPARE 15aec07e AS INSERT INTO public.t_bit_types (id, a, b) overriding system value VALUES ($1, $2, $3);
EXECUTE 15aec07e["2","100","101"];
COMMIT; -- {"xid":500,"lsn":"0/24E7090","timestamp":"2024-07-16 09:25:01.614482+0000"}
BEGIN; -- {"xid":501,"lsn":"0/24E7090","timestamp":"2024-07-16 09:25:01.614636+0000","commit_lsn":"0/24E7408"}
PREPARE 63eb6f2b AS INSERT INTO public.generated_column_test (id, name, greet_hello, greet_hi, "time", email, "table", """table""", """hel""lo""") overriding system value VALUES ($1, $2, DEFAULT, DEFAULT, DEFAULT, $3, DEFAULT, DEFAULT, DEFAULT), ($4, $5, DEFAULT, DEFAULT, DEFAULT, $6, DEFAULT, DEFAULT, DEFAULT), ($7, $8, DEFAULT, DEFAULT, DEFAULT, $9, DEFAULT, DEFAULT, DEFAULT);
EXECUTE 63eb6f2b["1","Tiger","tiger@wild.com","2","Elephant","elephant@wild.com","3","Cat","cat@home.net"];
COMMIT; -- {"xid":501,"lsn":"0/24E7408","timestamp":"2024-07-16 09:25:01.614636+0000"}
BEGIN; -- {"xid":502,"lsn":"0/24E7408","timestamp":"2024-07-16 09:25:01.614699+0000","commit_lsn":"0/24E75A8"}
PREPARE 24cd4492 AS UPDATE public.generated_column_test SET name = $1, greet_hello = DEFAULT, greet_hi = DEFAULT, "time" = DEFAULT, email = $2, "table" = DEFAULT, """table""" = DEFAULT, """hel""lo""" = DEFAULT WHERE id = $3;
EXECUTE 24cd4492["Lion","tiger@wild.com","1"];
PREPARE 24cd4492 AS UPDATE public.generated_column_test SET name = $1, greet_hello = DEFAULT, greet_hi = DEFAULT, "time" = DEFAULT, email = $2, "table" = DEFAULT, """table""" = DEFAULT, """hel""lo""" = DEFAULT WHERE id = $3;
EXECUTE 24cd4492["Lion","lion@wild.com","1"];
COMMIT; -- {"xid":502,"lsn":"0/24E75A8","timestamp":"2024-07-16 09:25:01.614699+0000"}
BEGIN; -- {"xid":503,"lsn":"0/24E75A8","timestamp":"2024-07-16 09:25:01.614739+0000","commit_lsn":"0/24E76A0"}
PREPARE 24cd4492 AS UPDATE public.generated_column_test SET name = $1, greet_hello = DEFAULT, greet_hi = DEFAULT, "time" = DEFAULT, email = $2, "table" = DEFAULT, """table""" = DEFAULT, """hel""lo""" = DEFAULT WHERE id = $3;
EXECUTE 24cd4492["Kitten","kitten@home.com","3"];
COMMIT; -- {"xid":503,"lsn":"0/24E76A0","timestamp":"2024-07-16 09:25:01.614739+0000"}
BEGIN; -- {"xid":504,"lsn":"0/24E76A0","timestamp":"2024-07-16 09:25:01.614770+0000","commit_lsn":"0/24E7720"}
PREPARE 763e46bc AS DELETE FROM public.generated_column_test WHERE id = $1;
EXECUTE 763e46bc["2"];
COMMIT; -- {"xid":504,"lsn":"0/24E7720","timestamp":"2024-07-16 09:25:01.614770+0000"}
-- KEEPALIVE {"lsn":"0/24E7720","timestamp":"2024-07-16 09:25:01.614784+0000"}
-- ENDPOS {"lsn":"0/24E7720"}
- ENDPOS {"lsn":"0/24E0198"}
