"""
Filter class that generates a filter file as described
in the pgcopydb documentation.

https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_config.html#filtering
"""

from collections import defaultdict

class Filter:

    EXCLUDE_EXTENSION = "exclude-extension"
    EXCLUDE_TABLE_DATA = "exclude-table-data"
    EXCLUDE_INDEX = "exclude-index"

    def __init__(self):
        self._filter = defaultdict(set)

    def exclude_extensions(self, extensions: list[str]):
        """
        Exclude extensions.
        """
        self._filter[self.EXCLUDE_EXTENSION].update(extensions)

    def _check_dot(self, relation: list[str]):
        for r in relation:
            if "." not in r:
                raise Exception(f"dot separator ('.') not found: {r}")

    def exclude_table_data(self, tables: list[str]):
        """
        Exclude table data from initial data migration.
        """
        self._check_dot(tables)
        self._filter[self.EXCLUDE_TABLE_DATA].update(tables)

    def exclude_indexes(self, indexes: list[str]):
        """
        Exclude indexes.
        """
        self._check_dot(indexes)
        self._filter[self.EXCLUDE_INDEX].update(indexes)

    def write(self, f):
        """
        Write filter to file as described in the pgcopydb documentation.
        """
        for section, contents in self._filter.items():
            f.write(f"[{section}]\n")

            for content in contents:
                f.write(f"{content}\n")

            f.write("\n")
