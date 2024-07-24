import logging

from dataclasses import dataclass
from multiprocessing import Pool

from catalog.pgcopydb import Catalog, Filter
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


@dataclass
class TableObjects:
    nspname: str
    relname: str
    obj_relid: int
    # if the constraint is backed by an index, this will be the index oid
    obj_idx_relid: int
    fq_table_name: str
    obj_name: str
    obj_definition: str
    is_compatible: bool
    kind: str
    obj_type: str

    def incompatible(self):
        return self.is_compatible == 'f'

    def __str__(self):
        return f"{self.kind} {self.obj_name}"

@dataclass
class Index(TableObjects):
    def definition(self):
        # Index definition is already in the obj_definition
        return self.obj_definition

@dataclass
class Constraint(TableObjects):
    def definition(self):
        return f"ALTER TABLE {self.fq_table_name} ADD CONSTRAINT {self.obj_name} {self.obj_definition}"

class Table:
    def __init__(self, sql_result):
        self.relname = sql_result['relname']
        self.nspname = sql_result['nspname']
        self.dimensions = sql_result['dimensions']

        self.indexes = []
        self.constraints = []
        self._has_incompatible_objs = False

    def __str__(self):
        return f"{self.nspname}.{self.relname}"

    def __repr__(self):
        return f"{self.nspname}.{self.relname} indexes: {self.indexes}, constraint_indexes: {self.constraints}"

    def _update_incompatible_objs(self, obj):
        self._has_incompatible_objs = self._has_incompatible_objs or obj.incompatible()

    def add_index(self, index):
        self.indexes.append(index)
        self._update_incompatible_objs(index)

    def add_constraint(self, constraint):
        self.constraints.append(constraint)
        self._update_incompatible_objs(constraint)

    def has_incompatible_objs(self):
        return self._has_incompatible_objs


def get_related_objs(pguri,
                     exclude_indexes: list[str],
                     table: Table) -> list[dict]:
    # Create VALUES clause for hypertable_filter CTE
    #         VALUES
    #         ('public', 'table1', '{id}'),
    #         ('public', 'table2', '{id, \"time\"}')
    # )

    hypertable_filter_sql = f"""
        VALUES ('{table.nspname}', '{table.relname}', '{table.dimensions}')
    """
    # Create VALUES clause for index_filter CTE
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
        VALUES {index_names}
    """

    sql = f"""
    WITH
    hypertable_filter (nspname, relname, dimensions) AS ({hypertable_filter_sql}),
    index_filter (faq_indexname) AS ({index_filter_sql}),
    hypertables AS (
        SELECT cls.oid, nsp.nspname, cls.relname, hf.dimensions
        FROM pg_class cls
        JOIN pg_namespace nsp ON cls.relnamespace = nsp.oid
        JOIN hypertable_filter hf ON hf.nspname = nsp.nspname AND hf.relname = cls.relname
    ), fk_to_hypertable AS (
        SELECT
           nsp.nspname,
           cls.relname,
           con.oid as obj_relid,
           FORMAT('%I.%I', nsp.nspname, cls.relname) as fq_table_name,
           FORMAT('%I', con.conname) AS constraint_name,
           pg_get_constraintdef(con.oid) AS constraint_definition,
           -- regular table should not have foreign key reference to hypertable
           false as is_compatible
        FROM
            pg_constraint con
        JOIN
            pg_class cls ON cls.oid = con.conrelid AND con.contype = 'f'
        JOIN
            pg_namespace nsp ON nsp.oid = cls.relnamespace
        JOIN
            hypertables ht ON ht.oid = con.confrelid
    ), fk_from_hypertable AS (
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
            hypertables ht ON ht.oid = con.conrelid AND con.contype = 'f'
    ), indexes AS (
        SELECT
            ht.nspname,
            ht.relname,
            idx.indexrelid as obj_relid,
            FORMAT('%I.%I', ht.nspname, ht.relname) as fq_table_name,
            FORMAT('%I', idxcls.relname) as index_name,
            pg_get_indexdef(idx.indexrelid) AS index_definition,
            CASE idx.indisprimary WHEN true THEN 'PRIMARY KEY' ELSE 'UNIQUE INDEX' END as kind,
            -- check if the index includes all hypertable dimensions, otherwise
            -- it will be treated as incompatible
            (array_agg(FORMAT('%I', att.attname::text) ORDER BY array_position(idx.indkey, att.attnum))) @> ht.dimensions::text[] as is_compatible
        FROM
            pg_index idx
        JOIN
            hypertables ht
            ON (ht.oid = idx.indrelid AND (idx.indisunique OR idx.indisprimary))
        JOIN
            pg_class idxcls ON idxcls.oid = idx.indexrelid
        JOIN
            pg_attribute att ON att.attrelid = idx.indrelid AND att.attnum = ANY(idx.indkey)
        LEFT JOIN index_filter idxf ON idxf.faq_indexname = FORMAT('%s.%s', ht.nspname, idxcls.relname)
        WHERE
            idxf.faq_indexname IS NULL
        GROUP BY
            ht.nspname, ht.relname, idx.indexrelid, idxcls.relname, ht.dimensions
        ORDER BY
            ht.nspname, ht.relname, idxcls.relname
    ), constraints AS (
        SELECT
            idx.nspname,
            idx.relname,
            con.oid as obj_relid,
            FORMAT('%I.%I', idx.nspname, idx.relname) as fq_table_name,
            FORMAT('%I', con.conname) AS constraint_name,
            pg_get_constraintdef(con.oid) AS constraint_definition,
            idx.obj_relid as obj_idx_relid,
            -- incompatible if the backing index is not compatible
            idx.is_compatible,
            CASE con.contype WHEN 'p' THEN 'PRIMARY KEY' ELSE 'UNIQUE CONSTRAINT' END as kind
        FROM
            pg_constraint con
        JOIN
            indexes idx ON idx.obj_relid = con.conindid AND con.contype IN ('p', 'u')
    )
    SELECT
        nspname,
        relname,
        obj_relid,
        obj_idx_relid,
        fq_table_name,
        constraint_name as obj_name,
        constraint_definition as obj_definition,
        is_compatible,
        kind,
        'constraint' as obj_type
    FROM constraints
    UNION ALL
    SELECT
        nspname,
        relname,
        obj_relid,
        NULL as obj_idx_relid,
        fq_table_name,
        index_name as obj_name,
        index_definition as obj_definition,
        is_compatible,
        kind,
        'index' as obj_type
    FROM indexes -- indexes that are not part of any constraint
    WHERE obj_relid NOT IN (SELECT obj_idx_relid FROM constraints)
    UNION ALL
    SELECT
        nspname,
        relname,
        obj_relid,
        NULL as obj_idx_relid,
        fq_table_name,
        constraint_name as obj_name,
        constraint_definition as obj_definition,
        is_compatible,
        'FOREIGN KEY' as kind,
        'constraint' as obj_type
    FROM fk_to_hypertable
    UNION ALL
    SELECT
        nspname,
        relname,
        obj_relid,
        NULL as obj_idx_relid,
        fq_table_name,
        constraint_name as obj_name,
        constraint_definition as obj_definition,
        is_compatible,
        'FOREIGN KEY' as kind,
        'constraint' as obj_type
    FROM fk_from_hypertable
    """
    return psql_cmd(conn=pguri, sql=sql)


# This could be an inner function, but it can't be pickled for multiprocessing.
def _create_index(args, index: Index):
    if index.incompatible() and args.skip_hypertable_incompatible_objects:
        logger.warn(f"Skipping incompatible index {index}")
        return True

    create_index = index.obj_definition

    sql = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = '{index.obj_name}' AND schemaname = '{index.nspname}') THEN
            EXECUTE '{create_index}';
        END IF;
    END $$;
    """
    logger.info(f"Creating index {create_index}")
    psql_cmd(conn=args.target, sql=sql)
    return True


# This could be an inner function, but it can't be pickled for multiprocessing.
def _create_constraint(args, hypertable: Table):
    for constraint in (hypertable.constraints):
        if constraint.incompatible() and args.skip_hypertable_incompatible_objects :
            logger.warn(f"Skipping incompatible constraint {constraint.obj_name}")
            continue

        # We need to check if the constraint already exists before creating it.
        constraint_definition = f"ALTER TABLE {constraint.fq_table_name} ADD CONSTRAINT {constraint.obj_name} {constraint.obj_definition}"
        sql = f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_namespace n ON(c.connamespace=n.oid) WHERE conname = '{constraint.obj_name}' AND nspname = '{constraint.nspname}') THEN
                EXECUTE '{constraint_definition}';
            END IF;
        END $$;
        """
        logger.info(f"Creating constraint {constraint_definition} if not exist...")
        psql_cmd(conn=args.target, sql=sql)
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
                indexes.append((args, index))

        pool.starmap(_create_index, indexes)

    # Building constraints sequentially on each table, but in
    # parallel across tables
    with Pool(processes=int(args.index_jobs)) as pool:
        # Prepare the list of hypertables with constraints to create
        hypertables = [(args, ht) for ht in hypertables]
        pool.starmap(_create_constraint, hypertables)

    # TODO: Should we run ANALYZE on the hypertables?


# HypertableCompatibility class is responsible for checking the compatibility of
# 1) Unique indexes
# 2) Unique constraints
# 3) Primary keys
# 4) Foreign keys
# on the tables that are to be converted to hypertables.
#
# How can we find an incompatible index?
# --------------------------------------
# Consider an index `I` on a table `T` that is to be converted to a hypertable. `I` should be one of the following:
#
# - A primary key
# - A unique index
# - A unique constraint
#
# `I` is deemed incompatible if it does not include all the columns that are part of the hypertable dimension.
#
# Non-unique indexes are always compatible.
#
# How can we find an incompatible foreign key?
# --------------------------------------------
# Foreign keys are deemed incompatible if the foreign key references a hypertable
# or a hypertable references to an another hypertable.
#
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

    def warn_incompatibility(self, error = True):
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
                if index.incompatible():
                    log_func(f"""{index} doesn't include hypertable """
                             f"dimension {hypertable.dimensions} as part of "
                             f"its definition.")

            for con in hypertable.constraints:
                if con.incompatible():
                    if con.obj_idx_relid:
                        log_func(f"""{con} doesn't include hypertable """
                                 f"dimension {hypertable.dimensions} as part of "
                                 f"its definition.")
                    else:
                        log_func(f"{con} is incompatible.")

        if incompatible and error:
            log_func(self._error_summary())
            return True
        return False

    def apply_filters(self, catalog: Catalog):
        # Filter out the indexes and constraints which are incompatible with
        # hypertables
        for ht in self.hypertables:
            for index in ht.indexes:
                catalog.remove_index(Filter(index.obj_relid, index.kind, index.obj_name))
            for con in ht.constraints:
                catalog.remove_constraint(Filter(con.obj_relid, con.kind, con.obj_name))
                # Filter the backing index of the constraint
                if con.obj_idx_relid:
                    catalog.remove_index(Filter(con.obj_idx_relid, con.kind, con.obj_name))

def create_hypertable_compatibility(args):
    hypertables = get_hypertable_dimensions(args.target)
    hypertable_objs = []
    # Get indexes and constraints for hypertables
    for ht in hypertables:
        table = Table(sql_result=ht)
        hypertable_objs.append(table)
        objs = get_related_objs(args.source, args.skip_index, table)

        for obj in objs:
            if obj['obj_type'] == 'index':
                table.add_index(Index(**obj))
            elif obj['obj_type'] == 'constraint':
                table.add_constraint(Constraint(**obj))

    return HypertableCompatibility(args, hypertable_objs)
