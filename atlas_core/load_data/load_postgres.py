from .utilities import (
    create_file_object,
    df_generator,
    logger,
    hdf_metadata,
    classification_to_pandas,
    cast_pandas,
    add_level_metadata,
)
from atlas_core import db

import pandas as pd

from sqlalchemy.schema import AddConstraint, DropConstraint
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.engine import reflection
from multiprocessing import Pool
from collections import defaultdict


def make_session():
    db.engine.dispose()
    session = sessionmaker(db.engine)
    s_session = scoped_session(session)()
    return s_session


def commit(session):
    session.commit()
    session.execute("SET maintenance_work_mem TO 1000000;")


def rollback(session):
    session.rollback()
    session.execute("SET maintenance_work_mem TO 1000000;")


def copy_to_database(session, table, columns, file_object):
    """
    Copy data to Postgres table using COPY command

    Parameters
    ----------
    session: SQLAlchemy session
    table: str
        destination SQL table name to copy to
    columns: list of str
        list of columns in table corresponding to data in file
    file_object: StringIO
        in-memory csv file to use to copy from
    """
    cur = session.connection().connection.cursor()
    columns = ", ".join([f"{col}" for col in columns])
    sql = f"COPY {table} ({columns}) FROM STDIN WITH CSV HEADER FREEZE"
    cur.copy_expert(sql=sql, file=file_object)


# Tried using this to update constant fields for a data set, but much slower
def update_level_fields(db, hdf_table, sql_table, levels):
    table_obj = db.metadata.tables[sql_table]
    update_obj = table_obj.update()

    for level, value in levels.get(hdf_table).items():
        col_name = level + "_level"
        update_obj = update_obj.values({col_name: value}).where(
            table_obj.c[col_name].is_(None)
        )

    return update_obj


def chunk_copy_df(session, df, sql_table, dest_chunksize):
    """
    Copy pandas dataframe to postgres table in iterative chunks

    Parameters
    ----------
    session: SQLAlchemy session
    df: pandas dataframe
    sql_table: str
    dest_chunksize: int
    """
    logger.info("Creating generator for chunking dataframe")
    for chunk in df_generator(df, dest_chunksize):

        logger.info("Creating CSV in memory")
        fo = create_file_object(chunk)

        logger.info("Copying chunk to database")
        copy_to_database(session, sql_table, df.columns, fo)
        del fo


def drop_foreign_keys(session):
    """
    Drop all foreign keys in a database.

    Parameters
    ----------
    session: SQLAlchemy db session
    logger: logging Logger object

    Returns
    -------
    db_foreign_keys: dict
        dict of sql table keys with list of ForeignKeyConstraint values
    """

    insp = reflection.Inspector.from_engine(db.engine)
    db_foreign_keys = {}
    for sql_table in insp.get_table_names():
        fks = db.metadata.tables[sql_table].foreign_key_constraints
        for fk in fks:
            try:
                session.execute(DropConstraint(fk))
                commit(session)
                logger.info("Dropped foreign key %s", fk.name)
            except SQLAlchemyError:
                logger.warn("Foreign key %s not found", fk.name)
                rollback(session)

        db_foreign_keys[sql_table] = fks

    return db_foreign_keys


def sort_tables(sql_to_hdf):
    """
    Returns sorted list of tables with classification tables in first positions
    and large partner tables next to distribute across threads

    Parameters
    ----------
    sql_to_hdf: dict
        List of dicts with sqltable: hdftable pairs

    Returns
    -------
    sorted_tables: dict
        Dict of lists of classification, partner, and other tables
    """

    sorted_tables = defaultdict(list)

    for sql, hdf in sql_to_hdf.items():
        if any("classifications/" in table for table in hdf):
            sorted_tables["classifications"].append(sql)
        elif any("partner" in table for table in hdf):
            sorted_tables["partner"].append(sql)
        else:
            sorted_tables["other"].append(sql)

    return sorted_tables


def prepare_table(session, sql_table):
    table_obj = db.metadata.tables[sql_table]

    # Drop PK for table
    logger.info("Dropping %s primary key", sql_table)
    pk = table_obj.primary_key
    session.execute(DropConstraint(pk))

    # Truncate SQL table
    logger.info("Truncating %s", sql_table)
    session.execute("TRUNCATE TABLE {};".format(sql_table))

    return table_obj, pk


def recreate_table_keys(session, sql_table, pk, foreign_keys):
    session = make_session()
    # Adding keys back to table
    logger.info("Recreating %s primary key", sql_table)
    session.execute(AddConstraint(pk))

    if foreign_keys:
        logger.info("Recreating foreign keys")
        for fk in foreign_keys:
            session.execute(AddConstraint(fk))


def copy_classifications(
    sql_table,
    sql_to_hdf,
    file_name,
    levels,
    source_chunksize,
    dest_chunksize,
    foreign_keys,
):
    rows = 0
    session = make_session()
    table_obj, pk = prepare_table(session, sql_table)

    # Copy all HDF tables related to SQL table
    hdf_tables = sql_to_hdf.get(sql_table)

    if hdf_tables is None:
        logger.warn("No HDF table found for SQL table %s", sql_table)
        return rows

    for hdf_table in hdf_tables:
        logger.info("*** %s ***", hdf_table)
        logger.info("Reading HDF table")
        df = pd.read_hdf(file_name, key=hdf_table)
        rows += len(df)

        logger.info("Formatting classification")
        df = classification_to_pandas(df)

        # Convert fields that should be int to object fields
        df = cast_pandas(df, table_obj)

        logger.info("Creating CSV in memory")
        fo = create_file_object(df)

        logger.info("Copying table to database")
        copy_to_database(session, sql_table, df.columns, fo)
        del df
        del fo

    logger.info("All chunks copied (%s rows)", rows)
    return rows

    recreate_table_keys(session, sql_table, pk, foreign_keys)

    logger.info("Committing transaction.")
    commit(session)
    session.close()


def copy_partner_table(
    sql_table,
    sql_to_hdf,
    file_name,
    levels,
    source_chunksize,
    dest_chunksize,
    foreign_keys,
):

    rows = 0
    session = make_session()
    table_obj, pk = prepare_table(session, sql_table)

    # Copy all HDF tables related to SQL table
    hdf_tables = sql_to_hdf.get(sql_table)

    if hdf_tables is None:
        logger.warn("No HDF table found for SQL table %s", sql_table)
        return rows

    for hdf_table in hdf_tables:
        logger.info("*** %s ***", hdf_table)
        hdf_levels = levels.get(hdf_table)

        with pd.HDFStore(file_name) as store:
            nrows = store.get_storer(hdf_table).nrows

        rows += nrows
        if nrows % source_chunksize:
            n_chunks = (nrows // source_chunksize) + 1
        else:
            n_chunks = nrows // source_chunksize

        start = 0

        for i in range(n_chunks):
            logger.info("*** HDF chunk %(i)s of %(n)s ***", {"i": i + 1, "n": n_chunks})

            logger.info("Reading HDF table")
            stop = min(start + source_chunksize, nrows)
            df = pd.read_hdf(file_name, key=hdf_table, start=start, stop=stop)

            start += source_chunksize

            # Handle NaN --> None type casting and adding const level data
            df = cast_pandas(df, table_obj)
            df = add_level_metadata(df, hdf_levels)

            chunk_copy_df(session, df, sql_table, dest_chunksize)
            del df

    logger.info("All chunks copied (%s rows)", rows)
    return rows


def copy_table(
    sql_table,
    sql_to_hdf,
    file_name,
    levels,
    source_chunksize,
    dest_chunksize,
    foreign_keys,
):

    rows = 0
    session = make_session()
    table_obj, pk = prepare_table(session, sql_table)

    # Copy all HDF tables related to SQL table
    hdf_tables = sql_to_hdf.get(sql_table)

    if hdf_tables is None:
        logger.warn("No HDF table found for SQL table %s", sql_table)
        return rows

    for hdf_table in hdf_tables:
        logger.info("*** %s ***", hdf_table)
        hdf_levels = levels.get(hdf_table)

        logger.info("Reading HDF table")
        df = pd.read_hdf(file_name, key=hdf_table)
        rows += len(df)

        # Handle NaN --> None type casting and adding const level data
        df = cast_pandas(df, table_obj)
        df = add_level_metadata(df, hdf_levels)

        chunk_copy_df(session, df, sql_table, dest_chunksize)
        del df

    logger.info("All chunks copied (%s rows)", rows)
    return rows


def hdf_to_postgres(
    file_name="./data.h5", keys=None, source_chunksize=10 ** 7, dest_chunksize=10 ** 6
):
    """
    Copy a HDF file to a postgres database

    Parameters
    ----------
    file_name: str
        path to a HDFfile to copy to database
    keys: iterable
        set of keys in HDF to limit load to
    source_chunksize: int
        when reading HDF file in chunks, max row count
    dest_chunksize: int
        max number of rows to read/copy in any transaction
    """

    session = make_session()
    session.execute("SET maintenance_work_mem TO 1000000;")

    logger.info("Compiling needed HDF metadata")
    sql_to_hdf, levels = hdf_metadata(file_name, keys)

    # Drop all foreign keys first to allow for dropping PKs after
    logger.info("Dropping foreign keys for all tables")
    db_foreign_keys = drop_foreign_keys(session)

    commit(session)
    session.close()

    sql_tables = sort_tables(sql_to_hdf)

    def gen_args(sql_tables):
        return [
            (
                sql_table,
                sql_to_hdf,
                file_name,
                levels,
                source_chunksize,
                dest_chunksize,
                db_foreign_keys.get(sql_table),
            )
            for sql_table in sql_tables
        ]

    try:
        p = Pool(3)
        p.starmap(copy_classifications, gen_args(sql_tables.get("classifications")))
        p.starmap(copy_partner_table, gen_args(sql_tables.get("partner")))
        p.starmap(copy_table, gen_args(sql_tables.get("other")))
    finally:
        p.close()
        p.join()
