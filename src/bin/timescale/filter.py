"""
Filter class that generates a filter file as described
in the pgcopydb documentation.

https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_config.html#filtering
"""

from collections import defaultdict

class Filter:

    EXCLUDE_EXTENSION = "exclude-extension"

    def __init__(self):
        self._filter = defaultdict(set)

    def exclude_extensions(self, extensions: list[str]):
        """
        Exclude extensions.
        """
        self._filter[self.EXCLUDE_EXTENSION].update(extensions)

    def write(self, f):
        """
        Write filter to file as described in the pgcopydb documentation.
        """
        for section, contents in self._filter.items():
            f.write(f"[{section}]\n")

            for content in contents:
                f.write(f"{content}\n")

            f.write("\n")
