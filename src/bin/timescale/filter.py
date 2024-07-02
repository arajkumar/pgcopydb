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
        for table in tables:
            if "." not in table:
                raise Exception(f"dot separator ('.') not found: {table}")
        self._filter[self.EXCLUDE_TABLE_DATA].update(tables)

    def write(self, f):
        """
        Write filter to file as described in the pgcopydb documentation.
        """
        # we want to preserve the order of the contents, because the new
        # sqlite based filtering system is order dependent and complains
        # if the filter contents are different on each run.
        sections = sorted(self._filter.keys())
        for section in sections:
            f.write(f"[{section}]\n")
            contents = sorted(self._filter[section])
            for content in contents:
                f.write(f"{content}\n")

            f.write("\n")
