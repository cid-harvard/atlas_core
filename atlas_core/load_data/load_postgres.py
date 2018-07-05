from .utilities import create_file_object, cast_pandas, logger,\
                       classification_to_pandas, hdf_metadata, df_generator
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


def drop_foreign_keys(session):
    '''
    Drop all foreign keys in a database.

    Parameters
    ----------
    session: SQLAlchemy db session

    Returns
    -------
    db_foreign_keys: list of ForeignKeyConstraint
        usable to iterate over to recreate keys after COPYing data
    '''

    insp = reflection.Inspector.from_engine(db.engine)
    db_foreign_keys = []
    for sql_table in insp.get_table_names():
        fks = db.metadata.tables[sql_table].foreign_key_constraints
        for fk in fks:
            session.execute(DropConstraint(fk))
        db_foreign_keys += fks

    return db_foreign_keys


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
        logger.info("*** %s ***", hdf_table)

        # Handle classifications differently
        if hdf_table.startswith("/classifications/"):
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

        # Read partner HDF tables as iterators to conserve memory
        elif 'partner' in hdf_table:
            hdf_levels = levels.get(hdf_table)

            logger.info("Reading HDF table %s", hdf_table)
            iterator = pd.read_hdf(file_name, key=hdf_table,
                                   chunksize=chunksize, iterator=True)

            n_chunks = (iterator.nrows // chunksize) + 1

            for i, df in enumerate(iterator):
                logger.info("*** Chunk %(i)s of %(n)s ***",
                            {'table': hdf_table, 'i': i + 1, 'n': n_chunks})
                rows += len(df)

                # Add columns from level metadata to df
                if hdf_levels:
                    logger.info("Adding level metadata values")
                    for entity, level_value in hdf_levels.items():
                        df[entity + "_level"] = level_value

                # Convert fields that should be int to object fields
                df = cast_pandas(df, table_obj)
                columns = df.columns

                logger.info("Creating CSV in memory")
                fo = create_file_object(df)
                del df

                logger.info("Copying table to database")
                copy_to_database(session, sql_table, columns, fo)
                del fo

        else:
            hdf_levels = levels.get(hdf_table)

            logger.info("Reading HDF table")
            df = pd.read_hdf(file_name, key=hdf_table)
            rows += len(df)

            # Convert fields that should be int to object fields
            df = cast_pandas(df, table_obj)

            # Add columns from level metadata to df
            if hdf_levels:
                logger.info("Adding level metadata values")
                for entity, level_value in hdf_levels.items():
                    df[entity + "_level"] = level_value

            # Split dataframe before creating StringIO objects in memory
            columns = df.columns

            for chunk in df_generator(df, chunksize, logger=logger):

                logger.info("Creating CSV in memory")
                fo = create_file_object(chunk)

                logger.info("Copying chunk to database")
                copy_to_database(session, sql_table, columns, fo)
                del fo

    logger.info("All chunks copied (%s rows)", rows)

    # Adding keys back to table
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
    db_foreign_keys = drop_foreign_keys(session)

    rows = 0

    for sql_table in sql_to_hdf.keys():
        rows += copy_to_postgres(sql_table, session, sql_to_hdf, file_name,
                                 levels, chunksize, commit_every)

    # Add foreign keys back in after all data loaded to not worry about order
    logger.info("Recreating foreign keys on all tables")
    for fk in db_foreign_keys:
        session.execute("SET maintenance_work_mem TO 1000000;")
        session.execute(AddConstraint(fk))
        session.commit()

    # Set this back to default value
    logger.info("Job complete. %s rows copied to db.", rows)
