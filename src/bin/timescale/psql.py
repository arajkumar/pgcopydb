import subprocess
import csv
from io import StringIO

from environ import env

def psql(conn, sql) -> list[dict]:
    """
    Execute the given SQL query using psql and return the result as a list of dictionaries.
    """
    command = ["psql", "-d", conn, "--csv", "-c", sql]

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