BEGIN;
    UPDATE metrics SET name = 'large numeric updated 1' WHERE id = 1;
    UPDATE metrics SET name = 'large numeric updated 2' WHERE id = 2;
COMMIT;

