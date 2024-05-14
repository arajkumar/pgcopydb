"""
Filter class that generates a filter file as described
in the pgcopydb documentation.

https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_config.html#filtering
"""

from collections import defaultdict

class Filter:

    EXCLUDE_EXTENSION = "exclude-extension"
    EXCLUDE_TABLE_DATA = "exclude-table-data"

    def __init__(self):
        self._filter = defaultdict(set)

    def exclude_extensions(self, extensions: list[str]):
        """
        Exclude extensions.
        """
        self._filter[self.EXCLUDE_EXTENSION].update(extensions)

    def exclude_table_data(self, tables: list[str]):
        """
        Exclude table data from initial data migration.
        """
        if len(tables) == 0:
            raise Exception("--exclude-table-data cannot be empty")
        for table in tables:
            if "." not in table:
                raise Exception(f"exclude-table-data: dot separator ('.') not found: {table}")
        self._filter[self.EXCLUDE_TABLE_DATA].update(tables)

    def write(self, f):
        """
        Write filter to file as described in the pgcopydb documentation.
        """
        for section, contents in self._filter.items():
            f.write(f"[{section}]\n")

            for content in contents:
                f.write(f"{content}\n")

            f.write("\n")
