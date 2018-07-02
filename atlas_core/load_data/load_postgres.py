from .utilities import create_file_object, cast_pandas, logger,\
                       classification_to_pandas, hdf_metadata
from atlas_core import db

import pandas as pd
import numpy as np

from sqlalchemy.schema import AddConstraint, DropConstraint
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import reflection


def copy_to_database(session, table, columns, file_object):
    cur = session.connection().connection.cursor()
    columns = ', '.join([f'{col}' for col in columns])
    sql = f'COPY {table} ({columns}) FROM STDIN WITH CSV HEADER FREEZE'
    cur.copy_expert(sql=sql, file=file_object)


# Tried using this to update constant fields for a data set, but much slower
def update_level_fields(db, hdf_table, sql_table, levels):
    table_obj = db.metadata.tables[sql_table]
    update_obj = table_obj.update()

    for level, value in levels.get(hdf_table).items():
        col_name = level + "_level"
        update_obj = update_obj.values({col_name: value})\
                               .where(table_obj.c[col_name].is_(None))

    return update_obj


def copy_to_postgres(sql_table, session, sql_to_hdf, file_name, levels,
                     chunksize, commit_every):
    '''Copy all HDF tables relating to a single SQL table to database'''

    table_obj = db.metadata.tables[sql_table]

    # Drop PK for table
    logger.info("Dropping %s primary key", sql_table)
    pk = table_obj.primary_key
    session.execute(DropConstraint(pk))

    # Truncate SQL table
    logger.info("Truncating %s", sql_table)
    session.execute('TRUNCATE TABLE {};'.format(sql_table))

    # Copy all HDF tables related to SQL table
    hdf_tables = sql_to_hdf.get(sql_table)

    rows = 0

    if hdf_tables is None:
        logger.info("No HDF table found for SQL table %s", sql_table)
        return rows

    for hdf_table in hdf_tables:
        logger.info("Reading HDF table %s", hdf_table)
        df = pd.read_hdf(file_name, key=hdf_table)
        rows += len(df)

        # Handle classifications differently
        if hdf_table.startswith("/classifications/"):
            logger.info("Formatting classification %s", hdf_table)
            df = classification_to_pandas(df)

        # Add columns from level metadata to df
        # Tried using UPDATE here but much slower than including in COPY
        if levels.get(sql_table):
            logger.info("Updating %s level fields", hdf_table)
            for entity, level_value in levels.get(hdf_table).items():
                df[entity + "_level"] = level_value

        columns = df.columns

        # Convert fields that should be int to object fields
        df = cast_pandas(df, table_obj)

        # Break large dataframes into managable chunks
        if len(df) > chunksize:
            n_arrays = (len(df) // chunksize) + 1
            split_dfs = np.array_split(df, n_arrays)
            del df

            for i, split_df in enumerate(split_dfs):
                logger.info(("Creating CSV in memory for %(table)s "
                             "(chunk %(i)s of %(n)s)"),
                            {'table': hdf_table, 'i': i + 1, 'n': n_arrays})
                fo = create_file_object(split_df)
                del split_df

                logger.info("Inserting %(table)s (chunk %(i)s of %(n)s)",
                            {'table': hdf_table, 'i': i + 1, 'n': n_arrays})
                copy_to_database(session, sql_table, columns, fo)
                del fo

        else:
            logger.info("Creating CSV in memory for %s", hdf_table)
            fo = create_file_object(df)
            del df

            logger.info("Inserting %s data", hdf_table)
            copy_to_database(session, sql_table, columns, fo)
            del fo

    # Adding keys back` to table
    logger.info("Recreating %s primary key", sql_table)
    session.execute(AddConstraint(pk))

    if commit_every:
        logger.info("Committing transaction.")
        session.commit()
        session.execute("SET maintenance_work_mem TO 1000000;")

    return rows


def hdf_to_postgres(file_name="./data.h5", chunksize=10**6, keys=None,
                    commit_every=True):
    '''
    Copy a HDF file to a postgres database

    Parameters
    ----------
    file_name: str
        path to a HDFfile to copy to database
    chunksize: int
        max number of rows to read/copy in any transaction
    keys: iterable
        set of keys in HDF to limit load to
    commit_every: boolean
        if true, commit after every major transaction (e.g., table COPY)
    '''

    session = db.session
    session.execute("SET maintenance_work_mem TO 1000000;")

    logger.info("Compiling needed HDF metadata")
    sql_to_hdf, levels = hdf_metadata(file_name, keys)

    # Drop all foreign keys first to allow for dropping PKs after
    logger.info("Dropping foreign keys for all tables")
    insp = reflection.Inspector.from_engine(db.engine)
    db_foreign_keys = []
    for sql_table in insp.get_table_names():
        fks = db.metadata.tables[sql_table].foreign_key_constraints
        for fk in fks:
            session.execute(DropConstraint(fk))
        db_foreign_keys += fks

    rows = 0

    for sql_table in sql_to_hdf.keys():
        rows += copy_to_postgres(sql_table, session, sql_to_hdf, file_name,
                                 levels, chunksize, commit_every)

    # Add foreign keys back in after all data loaded to not worry about order
    logger.info("Recreating foreign keys on all tables")
    session.execute("SET maintenance_work_mem TO 1000000;")
    for fk in db_foreign_keys:
        session.execute(AddConstraint(fk))

    # Set this back to default value
    session.commit()
    logger.info("Job complete. %s rows copied to db.", rows)
