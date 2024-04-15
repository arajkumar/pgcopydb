"""
Filter class that generates a filter file as described
in the pgcopydb documentation.

https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_config.html#filtering
"""
class Filter:
    def __init__(self, path: str = ""):
        self._path = path

    def exclude_materialized_view_refresh(self, matviews: list[str]):
        """
        Exclude materialized view refresh.
        """
        self._populate_filter("exclude-table-data", matviews)

    def _populate_filter(self, section: str, contents: list[str]):
        with open(self._path, "a") as f:
            f.write(f"[{section}]\n")
            for content in contents:
                f.write(f"{content}\n")
            f.write("\n")

    def path(self):
        return self._path
