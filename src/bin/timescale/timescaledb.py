import logging
import sys

from collections import defaultdict
from multiprocessing import Pool

from psql import psql as psql_cmd

logger = logging.getLogger(__name__)


def get_hypertable_dimensions(pguri) -> list[dict]:
    sql = """
select
    hypertable_schema as nspname,
    hypertable_name as relname,
    array_agg(FORMAT('%I', column_name::text)) as dimensions
from timescaledb_information.dimensions group by 1, 2;
 """
    dimensions = psql_cmd(conn=pguri,
                           sql=sql)
    return dimensions


class Hypertable:
    class Obj:
        def __init__(self, sql_result):
            # Update the object with the SQL result
            self.__dict__.update(sql_result)

            # Used for filtering
            self.restore_list_name = f"{self.nspname}.{self.relname}"
            self.obj_relid = sql_result['obj_relid']

        def compatible(self):
            if hasattr(self, 'is_compatible'):
                return self.is_compatible == 't'
            else:
                raise NotImplementedError

    class Index(Obj):
        def __init__(self, sql_result):
            super().__init__(sql_result)
            self.kind = 'INDEX'

            self.idxrelname = sql_result['idxrelname']
            self.columns = sql_result['columns']

        def __repr__(self):
            return f"UNIQUE INDEX {self.idxrelname}"

    class IndexConstraint(Obj):
        def __init__(self, sql_result):
            super().__init__(sql_result)

            self.kind = 'INDEX CONSTRAINT'
            self.constraint_name = sql_result['constraint_name']
            self.is_primary_key = sql_result['is_primary_key']
            self.obj_relid = sql_result['con_obj_relid']

        def __repr__(self):
            contype = 'PRIMARY KEY' if self.is_primary_key == 't' else 'UNIQUE CONSTRAINT'
            return f"{contype} {self.constraint_name}"

    class ForeignKey(Obj):
        def __init__(self, sql_result):
            super().__init__(sql_result)

            self.kind = 'FOREIGN KEY'

            self.constraint_name = sql_result['constraint_name']
            self.constraint_definition = sql_result['constraint_definition']

        def __repr__(self):
            return f"FOREIGN KEY {self.constraint_name}"

    def __init__(self, sql_result):
        # Update the object with the SQL result
        self.relname = sql_result['relname']
        self.nspname = sql_result['nspname']

        self.__dict__.update(sql_result)

        self.indexes = []
        self.constraint_indexes = []
        self.foreign_keys = []
        self._has_incompatible_objs = False

    def __str__(self):
        return f"{self.nspname}.{self.relname}"

    def __repr__(self):
        return f"{self.nspname}.{self.relname} indexes: {self.indexes}, constraint_indexes: {self.constraint_indexes}, foreign_keys: {self.foreign_keys}"

    def _update_incompatible_objs(self, obj):
        self._has_incompatible_objs = self._has_incompatible_objs or obj.compatible()

    def add_index(self, index):
        self.indexes.append(index)
        self._update_incompatible_objs(index)

    def add_constraint_index(self, index):
        self.constraint_indexes.append(index)
        self._update_incompatible_objs(index)

    def add_foreign_key(self, foreign_key):
        self.foreign_keys.append(foreign_key)
        self._update_incompatible_objs(foreign_key)

    def has_incompatible_objs(self):
        return self._has_incompatible_objs

def get_hypertable_indexes(pguri, hypertables: list[dict],
                           exclude_indexes) -> list[dict]:
    """
    Get list of unique indexes/unique or primary key constraints that are
    incompatible with hypertables dimensions.

    :param pguri: URI of the source database
    :param hypertables: List of hypertables
    :param exclude_indexes: List of fully qualified index names to exclude
    :return: Dictionary where key is the table name and value is the list of incompatible indexes/constraints
    """


    # Create a CTE named hypertable_filter to filter hypertables
    # e.g. hypertable_filter(nspname, relname, dimensions) as(
    #         VALUES
    #         ('public', 'table1', '{id}'),
    #         ('public', 'table2', '{id, \"time\"}')
    # )
    hypertable_names = map(lambda row: f"""
(
'{row['nspname']}', '{row['relname']}', '{row['dimensions']}'
)""", hypertables)
    hypertable_names = ", ".join(hypertable_names)

    hypertable_filter_sql = f"""
hypertable_filter(nspname, relname, dimensions) as (
        VALUES {hypertable_names}
)
"""

    # Create a CTE named index_filter to filter indexes
    # e.g. index_filter(faq_indexname) as(
    #         VALUES
    #         ('public.index1'),
    #         ('public.index2')
    # )
    if exclude_indexes:
        index_names = map(lambda index: f"('{index}')", exclude_indexes)
        index_names = ", ".join(index_names)
    else:
        index_names = "(NULL)"

    index_filter_sql = f"""
index_filter(faq_indexname) as (
        VALUES {index_names}
)
"""
    sql = f"""
WITH {index_filter_sql}, {hypertable_filter_sql}, indexes AS (
    SELECT
        ns.nspname AS nspname,
        cls.relname AS relname,
        idxns.nspname AS idxnspname,
        idxcls.relname AS idxrelname,
        am.amname AS index_type,
        array_agg(FORMAT('%I', att.attname::text) ORDER BY array_position(idx.indkey, att.attnum)) AS columns,
        pg_get_indexdef(idx.indexrelid) AS index_definition,
        con.conname AS constraint_name,
        CASE WHEN con.oid IS NOT NULL THEN pg_get_constraintdef(con.oid) ELSE NULL END AS constraint_definition,
        idx.indexrelid as obj_relid,
        CASE WHEN con.oid IS NOT NULL THEN con.oid ELSE 0 END as con_obj_relid,
        idx.indisprimary as is_primary_key
    FROM
        pg_index idx
    JOIN
        pg_class cls ON cls.oid = idx.indrelid
    JOIN
        pg_namespace ns ON ns.oid = cls.relnamespace
    JOIN
        pg_class idxcls ON idxcls.oid = idx.indexrelid
    JOIN
        pg_namespace idxns ON idxns.oid = idxcls.relnamespace
    JOIN
        pg_attribute att ON att.attrelid = idx.indrelid AND att.attnum = ANY(idx.indkey)
    JOIN
        pg_am am ON idxcls.relam = am.oid
    LEFT JOIN
        pg_constraint con ON con.conindid = idx.indexrelid
    WHERE
        (idx.indisunique OR idx.indisprimary)
    GROUP BY
        ns.nspname, cls.relname, idxns.nspname, idxcls.relname, am.amname, idx.indisunique, idx.indexrelid, con.conname, con.oid
    ORDER BY
        ns.nspname, cls.relname, idxns.nspname, idxcls.relname
)
SELECT
    i.nspname,
    i.relname,
    i.idxnspname,
    i.idxrelname,
    i.obj_relid,
    i.con_obj_relid,
    FORMAT('%I.%I', i.nspname, i.relname) as fq_table_name,
    FORMAT('%I.%I', i.idxnspname, i.idxrelname) as fq_index_name,
    columns,
    index_definition,
    CASE WHEN constraint_name IS NOT NULL THEN FORMAT('%I', constraint_name) ELSE NULL END as constraint_name,
    constraint_definition,
    is_primary_key,
    i.columns @> hf.dimensions::text[] as is_compatible
FROM indexes i
JOIN hypertable_filter hf ON hf.nspname = i.nspname AND hf.relname = i.relname
LEFT JOIN index_filter idxf ON idxf.faq_indexname = FORMAT('%s.%s', i.idxnspname, i.idxrelname)
WHERE idxf.faq_indexname IS NULL
"""
    return psql_cmd(conn=pguri, sql=sql)


def get_hypertale_foreign_keys(pguri, hypertables: list[dict]) -> list[dict]:
    # Create VALUES clause for hypertable_filter CTE
    #         VALUES
    #         ('public', 'table1', '{id}'),
    #         ('public', 'table2', '{id, \"time\"}')
    # )
    hypertable_names = map(lambda row: f"""
(
'{row['nspname']}', '{row['relname']}', '{row['dimensions']}'
)""", hypertables)
    hypertable_names = ", ".join(hypertable_names)

    hypertable_filter_sql = f"""
        VALUES {hypertable_names}
"""
    sql = f"""
WITH
hypertable_filter (nspname, relname, dimensions) AS ({hypertable_filter_sql}),
hypertables AS (
        SELECT cls.oid, nsp.nspname, cls.relname
        FROM pg_class cls
        JOIN pg_namespace nsp ON cls.relnamespace = nsp.oid
        JOIN hypertable_filter hf ON hf.nspname = nsp.nspname AND hf.relname = cls.relname
)
SELECT
    ht.nspname,
    ht.relname,
    con.oid as obj_relid,
    FORMAT('%I.%I', ht.nspname, ht.relname) as fq_table_name,
    FORMAT('%I', con.conname) AS constraint_name,
    pg_get_constraintdef(con.oid) AS constraint_definition,
    -- hypertable should not have foreign key reference to other hypertable
    NOT EXISTS (SELECT 1 FROM hypertables WHERE oid = con.confrelid) as is_compatible
FROM
    pg_constraint con
JOIN
    hypertables ht ON ht.oid = con.conrelid
WHERE
    con.contype = 'f'
"""
    return psql_cmd(conn=pguri, sql=sql)


# This could be an inner function, but it can't be pickled for multiprocessing.
def _create_index(args, index):
    create_index = index.index_definition

    sql = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = '{index.idxrelname}' AND schemaname = '{index.idxnspname}') THEN
            EXECUTE '{create_index}';
        END IF;
    END $$;
    """
    logger.info(f"Creating index {create_index}")
    psql_cmd(conn=args.target, sql=sql)
    return True


# This could be an inner function, but it can't be pickled for multiprocessing.
def _filter_constraint(args, hypertable):
    for constraint in hypertable.constraint_indexes:
        if args.skip_hypertable_incompatible_objects and not constraint.compatible():
            logger.warn(f"Skipping incompatible constraint {constraint['constraint_name']}")
            continue

        # We need to check if the constraint already exists before creating it.
        constraint_definition = f"ALTER TABLE {constraint.fq_table_name} ADD CONSTRAINT {constraint.constraint_name} {constraint.constraint_definition}"
        sql = f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_namespace n ON(c.connamespace=n.oid) WHERE conname = '{constraint.constraint_name}' AND nspname = '{constraint.nspname}') THEN
                EXECUTE '{constraint_definition}';
            END IF;
        END $$;
        """
        logger.info(f"Creating constraint {constraint_definition} if not exist...")
        psql_cmd(conn=args.target, sql=sql)
    return True


def _create_foreign_key(args, hypertable):
    for fk in hypertable.foreign_keys:
        create_fk = f"ALTER TABLE {fk.fq_table_name} ADD CONSTRAINT {fk.constraint_name} {fk.constraint_definition}"
        logger.info(f"Creating foreign key {create_fk}")
        psql_cmd(conn=args.target, sql=create_fk)
    return True

def try_creating_incompatible_objects(args, hypertables):
    logger.info("Creating indexes and constraints on the target hypertable ...")

    # First build all indexes in parallel followed by constraints. Otherwise,
    # constraint building will block the index building as it takes table lock.

    # Building indexes in parallel across tables
    with Pool(processes=int(args.index_jobs)) as pool:
        # Prepare the list of indexes to create, excluding constraints.
        indexes = []
        for hypertable in hypertables:
            for index in hypertable.indexes:
                if not index.compatible() and args.skip_hypertable_incompatible_objects:
                    logger.warn(f"Skipping incompatible index {index}")
                else:
                    indexes.append((args, index))

        pool.starmap(_create_index, indexes)

    # Building constraints sequentially on each table, but in
    # parallel across tables
    with Pool(processes=int(args.index_jobs)) as pool:
        pool.starmap(_filter_constraint, hypertables)

    # Building foreign keys sequentially on each table, but in
    # parallel across tables
    with Pool(processes=int(args.index_jobs)) as pool:
        pool.starmap(_create_foreign_key, hypertables)

    # TODO: Should we run ANALYZE on the hypertables?


class HypertableCompatibility:
    def __init__(self, args, hypertables):
        self.args = args
        self.hypertables = hypertables

    def create_incompatible_objects(self):
        try_creating_incompatible_objects(self.args, self.hypertables)

    def _error_summary(self):
        message = f"""
{"*" * 72}
You can do one of the following to resolve the issue:
    1) Skip the compatibility check using the `--skip-hypertable-compatibility-check` flag and manually create the indexes/constraints on the target when the tool fails to create them. This is a most optimal way to resolve the issue.
    2) Fix the incompatible indexes/constraints on the source to include hypertable dimensions and restart the migration from the beginning.
    3) Skip the incompatible indexes/constraints using the `--skip-hypertable-incompatible-objects` flag. Beware, skipping them might slow down the replication of the UPDATE/DELETE operations on the target.
    4) Skip the incompatible indexes/constraints using the `--skip-index` flag and resume the migration.
{"*" * 72}
    """
        return message

    def warn_hypertable_incompatibility(self, error = True):
        if error:
            log_func = logger.error
        else:
            log_func = logger.warn

        incompatible = False

        for hypertable in self.hypertables:
            if hypertable.has_incompatible_objs():
                log_func(f"Table {hypertable} has incompatible indexes/constraints")
                incompatible = True

            for index in hypertable.indexes:
                if index.is_compatible == 'f':
                    log_func(f"""{index} doesn't include hypertable """
                             f"dimension {hypertable.dimensions} as part of "
                             f"its definition.")
            for index in hypertable.constraint_indexes:
                if index.is_compatible == 'f':
                    log_func(f"""{index} doesn't include hypertable """
                             f"dimension {hypertable.dimensions} as part of "
                             f"its definition.")
            for fk in hypertable.foreign_keys:
                if fk.is_compatible == 'f':
                    log_func(f"{fk} is incompatible.")

        if incompatible and error:
            log_func(self._error_summary())
            return True
        return False


    def indexes_to_skip(self):
        skip_indexes = []
        for ht in self.hypertables:
            # The constraints which we deal here are the unique constraints
            # and primary keys, which are also backed by indexes. Hence, we
            # can skip the constraints by using its index name.
            skip_indexes.extend(ht.indexes + ht.constraint_indexes)
        return skip_indexes

    def foreign_keys_to_skip(self):
        skip_foreign_keys = []
        for ht in self.hypertables:
           skip_foreign_keys.extend(ht.foreign_keys)
        return skip_foreign_keys

def check_hypertable_incompatibility(args):
    hypertables = get_hypertable_dimensions(args.target)

    hypertable_objs = {}
    # Create Hypertable objects
    for row in hypertables:
        fq_table_name = f"{row['nspname']}.{row['relname']}"
        hypertable_objs[fq_table_name] = hypertable_objs.get(fq_table_name, Hypertable(row))

    # Get indexes and constraints for hypertables
    indexes = get_hypertable_indexes(args.source, hypertables, args.skip_index)
    for row in indexes:
        fq_table_name = f"{row['nspname']}.{row['relname']}"
        ht = hypertable_objs[fq_table_name]
        if row['constraint_name']:
            ht.add_constraint_index(Hypertable.IndexConstraint(row))
            ht.add_index(Hypertable.Index(row))
        else:
            ht.add_index(Hypertable.Index(row))

    # Get foreign keys for hypertables
    foreign_keys = get_hypertale_foreign_keys(args.source, hypertables)
    for row in foreign_keys:
        fq_table_name = f"{row['nspname']}.{row['relname']}"
        ht = hypertable_objs[fq_table_name]
        print(row)
        ht.add_foreign_key(Hypertable.ForeignKey(row))

    return HypertableCompatibility(args, list(hypertable_objs.values()))
