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


def get_hypertable_incompatible_objects(pguri, hypertables: list[dict],
                                        exclude_indexes) -> defaultdict[str, list[dict]]:
    """
    Get list of indexes/constraints that are incompatible with hypertables.

    :param pguri: URI of the source database
    :param hypertables: List of hypertables
    :param exclude_indexes: List of fully qualified index names to exclude
    :return: Dictionary where key is the table name and value is the list of incompatible indexes/constraints
    """

    # If there are no hypertables, there are no incompatible objects.
    if not hypertables:
        logger.warn("No hypertables found in the target database.")
        return defaultdict(list)

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
    result = psql_cmd(conn=pguri, sql=sql)

    group_dimension = {}
    for row in hypertables:
        fq_table_name = f"{row['nspname']}.{row['relname']}"
        group_dimension[fq_table_name] = row['dimensions']

    group_by_table = defaultdict(list)
    for row in result:
        fq_table_name = f"{row['nspname']}.{row['relname']}"
        row['dimensions'] = group_dimension[fq_table_name]
        group_by_table[fq_table_name].append(row)

    return group_by_table


# This could be an inner function, but it can't be pickled for multiprocessing.
def _create_index(index, args):

    if args.skip_hypertable_incompatible_objects and index['is_compatible'] == 'f':
        logger.warn(f"Skipping incompatible index {index['fq_index_name']}")
        return True

    create_index = index['index_definition']

    sql = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = '{index['idxrelname']}' AND schemaname = '{index['idxnspname']}') THEN
            EXECUTE '{create_index}';
        END IF;
    END $$;
    """
    logger.info(f"Creating index {create_index}")
    psql_cmd(conn=args.target, sql=sql)
    return True


# This could be an inner function, but it can't be pickled for multiprocessing.
def _filter_constraint(table_name, args, hypertable_info):
    objs = hypertable_info[table_name]
    objs = filter(lambda i: i['constraint_name'], objs)
    for constraint in objs:
        if args.skip_hypertable_incompatible_objects and constraint['is_compatible'] == 'f':
            logger.warn(f"Skipping incompatible constraint {constraint['constraint_name']}")
            continue

        # Unlike indexes, constraints can't be created with IF NOT EXISTS.
        # We need to check if the constraint already exists before creating it.
        constraint_definition = f"ALTER TABLE {constraint['fq_table_name']} ADD CONSTRAINT {constraint['constraint_name']} {constraint['constraint_definition']}"
        sql = f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_namespace n ON(c.connamespace=n.oid) WHERE conname = '{constraint['constraint_name']}' AND nspname = '{constraint['nspname']}') THEN
                EXECUTE '{constraint_definition}';
            END IF;
        END $$;
        """
        logger.info(f"Creating constraint {constraint_definition} if not exist...")
        psql_cmd(conn=args.target, sql=sql)
    return True


def try_creating_incompatible_objects(args, hypertable_info):
    logger.info("Creating indexes and constraints on the target hypertable ...")

    # First build all indexes in parallel followed by constraints. Otherwise,
    # constraint building will block the index building as it takes table lock.

    # Building indexes in parallel across tables
    with Pool(processes=int(args.index_jobs)) as pool:
        # Prepare the list of indexes to create, excluding constraints.
        indexes = []
        for table, info in hypertable_info.items():
            for obj in info:
                if not obj['constraint_name']:
                    indexes.append((obj, args))
        pool.starmap(_create_index, indexes)

    # Building constraints sequentially on each table, but in
    # parallel across tables
    with Pool(processes=int(args.index_jobs)) as pool:
        args = [(table, args, hypertable_info) for table in hypertable_info.keys()]
        pool.starmap(_filter_constraint, args)

    # TODO: Should we run ANALYZE on the hypertables?


def show_hypertable_incompatibility(hypertable_info, error = True):
    if error:
        log_func = logger.error
    else:
        log_func = logger.warn

    incompatible = False
    for table, info in hypertable_info.items():
        print_table_name = True
        for i in info:
            if i['is_compatible'] == 'f':
                idx_type = ''
                nspname = i['nspname']
                if i['constraint_name']:
                    is_primary_key = i['is_primary_key'] == 't'
                    idx_type = 'PRIMARY KEY' if is_primary_key else 'UNIQUE CONSTRAINT'
                    idxname = i['constraint_name']
                else:
                    idx_type = 'UNIQUE INDEX'
                    idxname = i['idxrelname']
                dimensions = i['dimensions']

                if print_table_name:
                    log_func(f"Table {table} has incompatible constraints/indexes:")
                    print_table_name = False

                log_func(f"""{nspname}.{idxname} {idx_type} doesn't include hypertable """
                         f"dimension {dimensions} as part of "
                         f"its definition.")

                incompatible = True

    if incompatible and error:
        message = f"""
{"*" * 72}
You can do one of the following to resolve the issue:
    1) Skip the compatibility check using the `--skip-hypertable-compatibility-check` flag and manually create the indexes/constraints on the target when the tool fails to create them. This is a most optimal way to resolve the issue.
    2) Fix the incompatible indexes/constraints on the source to include hypertable dimensions and restart the migration from the beginning.
    3) Skip the incompatible indexes/constraints using the `--skip-hypertable-incompatible-objects` flag. Beware, skipping them might slow down the replication of the UPDATE/DELETE operations on the target.
    4) Skip the incompatible indexes/constraints using the `--skip-index` flag and resume the migration.
{"*" * 72}
"""
        log_func(message)


def check_hypertable_incompatibility(args):
    dimensions = get_hypertable_dimensions(args.target)

    incompatible_tables = get_hypertable_incompatible_objects(args.source, dimensions, args.skip_index)

    return incompatible_tables


def hypertable_has_incompatible_objects(hypertable_info):
    for _, info in hypertable_info.items():
        for i in info:
            if i['is_compatible'] == 'f':
                return True

    return False


def filter_incompatible_index_constraint(incompatible_tables):
    incompatible_indexes = []
    for _, info in incompatible_tables.items():
        for i in info:
            nspname = i['nspname']
            idxname = i['idxrelname']
            indexname = f"{nspname}.{idxname}"
            incompatible_indexes.append(indexname)

    return incompatible_indexes


def warn_hypertable_incompatibility(args, hypertable_info):
    if hypertable_has_incompatible_objects(hypertable_info):
        if args.skip_hypertable_incompatible_objects or args.skip_hypertable_compatibility_check:
            show_hypertable_incompatibility(hypertable_info, error=False)
        else:
            show_hypertable_incompatibility(hypertable_info, error=True)
            sys.exit(1)
