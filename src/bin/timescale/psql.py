import subprocess
import csv
from io import StringIO

from environ import env

# TODO: Use the `psycopg` library to execute SQL queries instead of using
# the `psql` command.
def psql(conn, sql, **kwargs) -> list[dict]:
    """
    Execute the given SQL query using psql and return the result as a list of dictionaries.
    """
    if kwargs.get('snapshot'):
        sql = f"""
        BEGIN;
        SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY, DEFERRABLE;
        SET TRANSACTION SNAPSHOT '{kwargs['snapshot']}';
        {sql}
        COMMIT;
        """

    command = ["psql", "--dbname", conn, "--quiet", "--csv", "--command", sql]

    try:
        result = subprocess.check_output(command,
                                         stderr=subprocess.PIPE,
                                         universal_newlines=True,
                                         env=env)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error executing SQL: {e.stderr}")

    reader = csv.DictReader(StringIO(result), delimiter=',')
    # Convert the reader to a list to avoid the generator being exhausted
    return list(reader)

# test
if __name__ == "__main__":
    sql = """
    BEGIN;
    SELECT 1 AS one;
    SELECT 2 AS two;
    SELECT 3 AS three;
    """
    print(psql(env['PGCOPYDB_TARGET_PGURI'], sql))
