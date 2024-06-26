BEGIN;
    UPDATE metrics SET name = 'NaN updated', value = 12345 WHERE value = 'NaN';
    UPDATE metrics SET name = 'Infinity updated', value = 54321 WHERE value = '+Infinity';
    UPDATE metrics SET name = '-Infinity updated', value = 67890 WHERE value = '-Infinity';
COMMIT;

