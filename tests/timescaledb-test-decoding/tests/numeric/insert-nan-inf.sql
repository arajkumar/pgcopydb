BEGIN;
    INSERT INTO metrics (id, time, name, value) VALUES
    (1, now() - '1h'::interval, 'NaN', 'NaN'),
    (2, now() + '1h'::interval, 'Infinity', '+Infinity'),
    (3, now() + '3h'::interval, '-Infinity', '-Infinity');
COMMIT;
