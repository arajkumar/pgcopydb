import sqlite3

from dataclasses import dataclass, asdict
from pathlib import Path

@dataclass
class Filter:
    oid: int
    kind: str
    restore_list_name: str

class Catalog:
    def __init__(self, dir: Path):
        self.source_db = str(dir / "schema" / "source.db")
        self.filter_db = str(dir / "schema" / "filter.db")
        self.filter_indexes:list[dict] = list()
        self.filter_constraints: list[dict] = list()

    def remove_index(self, index: Filter):
        """
        Remove index from the filter database.
        """
        self.filter_indexes.append(asdict(index))

    def remove_constraint(self, constraint: Filter):
        """
        Remove constraint from the filter database.
        """
        self.filter_constraints.append(asdict(constraint))

    def update(self):
        """
        Update the filter database with the indexes and constraints to be removed.
        """
        # Connect to the source
        source_conn = sqlite3.connect(self.source_db)
        # Attach the filter database
        source_conn.execute("ATTACH DATABASE ? AS filter", (self.filter_db,))

        source_conn.execute("BEGIN")
        if self.filter_indexes:
            source_conn.executemany("DELETE FROM s_index WHERE oid = :oid",
                                            self.filter_indexes)
        if self.filter_constraints:
            source_conn.executemany("DELETE FROM s_constraint WHERE oid = :oid",
                                            self.filter_constraints)
        source_conn.executemany("INSERT OR REPLACE INTO filter.filter VALUES (:oid, :kind, :restore_list_name)", self.filter_constraints + self.filter_indexes)
        source_conn.commit()
        source_conn.close()
