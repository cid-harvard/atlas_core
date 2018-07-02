"""Import from an ingested .hdf file to an sql database."""
import logging
from collections import defaultdict
from io import StringIO

from sqlalchemy.schema import AddConstraint, DropConstraint
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import reflection

from atlas_core import db

# Add new formatted log handler for data_import
logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s %(asctime)s.%(msecs)03d %(message)s",
    datefmt="%Y-%m-%d,%H:%M:%S"
)
logger = logging.getLogger('data_import')


# Adapted from https://gist.github.com/mangecoeur/1fbd63d4758c2ba0c470#gistcomment-2086007
def create_file_object(df):
    """Creates a csv file or writes to memory"""
    s_buf = StringIO()
    df.to_csv(s_buf, index=False)
    s_buf.seek(0)
    return s_buf


def copy_to_database(session, table, columns, file_object):
    cur = session.connection().connection.cursor()
    columns = ', '.join([f'{col}' for col in columns])
    sql = f'COPY {table} ({columns}) FROM STDIN WITH CSV HEADER FREEZE'
    cur.copy_expert(sql=sql, file=file_object)


# Tried using this to update constant fields for a data set, but much slower
def update_level_fields(db, sql_table, levels):
    table_obj = db.metadata.tables[sql_table]
    update_obj = table_obj.update()

    for level, value in levels.get(sql_table).items():
        col_name = level + "_level"
        update_obj = update_obj.values({col_name: value})\
                               .where(table_obj.c[col_name].is_(None))

    return update_obj


# Convert fields that should be ints in the db to pandas object fields since
# pandas ints cannot handle NaN. np.NaN is also not comparable, hence the str
def cast_pandas(df, table_obj):
    for col in table_obj.columns:
        if str(col.type) in ['INTEGER', 'BIGINT']:
            df[col.name] = df[col.name].apply(
                lambda x: None if str(x) == 'nan' else int(x),
                convert_dtype=False
            )
        elif str(col.type) == 'BOOLEAN':
            df[col.name] = df[col.name].apply(
                lambda x: None if str(x) == 'nan' else bool(x),
                convert_dtype=False
            )

    return df


def classification_to_pandas(df, optional_fields=["name_es", "name_short_en",
                                                  "name_short_es",
                                                  "description_en",
                                                  "description_es",
                                                  "is_trusted",
                                                  "in_rankings"
                                                  ]):
    """Convert a classification from the format it comes in the classification
    file (which is the format from the 'classifications' github repository)
    into the format that the flask apps use. Mostly just a thing for dropping
    unneeded columns and renaming existing ones.

    The optional_fields allows you to specify which fields should be considered
    optional, i.e. it'll still work if this field doesn't exist in the
    classification, like the description fields for example.
    """

    # Sort fields and change names appropriately
    new_df = df[["index", "code", "name", "level", "parent_id"]]
    new_df = new_df.rename(columns={
        "index": "id",
        "name": "name_en"
    })

    for field in optional_fields:
        if field in df:
            new_df[field] = df[field]

    return new_df


def import_data_sqlite(file_name="./data.h5", engine=None,
                       source_chunksize=10**6, dest_chunksize=10**6, keys=None):
    # Keeping this import inlined to avoid a dependency unless needed
    import pandas as pd

    print("Reading from file:'{}'".format(file_name))
    store = pd.HDFStore(file_name, mode="r")

    if keys is None:
        keys = store.keys()

    for key in keys:
        print("-----------------------------------")
        print("HDF Table: {}".format(key))

        try:
            metadata = store.get_storer(key).attrs.atlas_metadata
            print("Metadata: {}".format(metadata))
        except AttributeError:
            print("Skipping {}".format(key))
            continue

        table_name = metadata.get("sql_table_name", None)
        print("SQL Table: {}".format(table_name))

        if table_name is None:
            print("Skipping {}".format(key))
            continue

        try:
            if key.startswith("/classifications/"):
                df = pd.read_hdf(file_name, key=key)
                df = classification_to_pandas(df)
                df.to_sql(table_name, engine, index=False,
                          chunksize=dest_chunksize, if_exists="append")

            else:
                # If it's a timeseries data table, load it in chunks to not
                # exhaust memory all at once
                iterator = pd.read_hdf(file_name, key=key,
                                       chunksize=source_chunksize,
                                       iterator=True)

                for i, df in enumerate(iterator):
                    print(i * source_chunksize)

                    # Add in level fields
                    if "levels" in metadata:
                        for entity, level_value in metadata["levels"].items():
                            df[entity+"_level"] = level_value

                    df.to_sql(table_name, engine, index=False,
                              chunksize=dest_chunksize, if_exists="append")

                    # Hint that this object should be garbage collected
                    del df

        except SQLAlchemyError as exc:
            print(exc)


def copy_to_postgres(session, sql_table, sql_to_hdf, file_name, levels, chunksize):

    import pandas as pd
    import numpy as np

    table_obj = db.metadata.tables[sql_table]

    # Drop PK for table
    logger.info("Dropping {} primary key".format(sql_table))
    pk = table_obj.primary_key
    session.execute(DropConstraint(pk))

    # Truncate SQL table
    logger.info("Truncating {}".format(sql_table))
    session.execute('TRUNCATE TABLE {};'.format(sql_table))

    # Copy all HDF tables related to SQL table
    hdf_tables = sql_to_hdf.get(sql_table)

    rows = 0

    if hdf_tables is None:
        logger.info("No HDF table found for SQL table {}".format(sql_table))
        return rows

    for hdf_table in hdf_tables:
        logger.info("Reading HDF table {}".format(hdf_table))
        df = pd.read_hdf(file_name, key=hdf_table)
        rows += len(df)

        # Handle classifications differently
        if hdf_table.startswith("/classifications/"):
            logger.info("Formatting classification {}".format(hdf_table))
            df = classification_to_pandas(df)

        # Add columns from level metadata to df
        # Tried using UPDATE here but much slower than including in COPY
        if levels.get(sql_table):
            logger.info("Updating {} level fields".format(hdf_table))
            for entity, level_value in levels.get(sql_table).items():
                df[entity+"_level"] = level_value

        columns = df.columns

        # Convert fields that should be int to object fields
        df = handle_pandas_ints(df, table_obj)

        # Break large dataframes into managable chunks
        if len(df) > chunksize:
            n_arrays = (len(df) // chunksize) + 1
            split_dfs = np.array_split(df, n_arrays)
            del df

            for i, split_df in enumerate(split_dfs):
                logger.info("Creating CSV in memory for {} chunk {} of {}"
                            .format(hdf_table, i + 1, n_arrays))
                fo = create_file_object(split_df)
                del split_df

                logger.info("Inserting {} chunk {} of {}"
                            .format(hdf_table, i + 1, n_arrays))
                copy_to_database(session, sql_table, columns, fo)
                del fo

        else:
            logger.info("Creating CSV in memory for {}".format(hdf_table))
            fo = create_file_object(df)
            del df

            logger.info("Inserting {} data".format(hdf_table))
            copy_to_database(session, sql_table, columns, fo)
            del fo

    # Adding keys back to table
    logger.info("Recreating {} primary key".format(sql_table))
    session.execute(AddConstraint(pk))
    session.commit()

    return rows


def import_data_postgres(file_name="./data.h5", chunksize=10**6, keys=None):

    # Keeping this import inlined to avoid a dependency unless needed
    import pandas as pd

    session = db.session

    logger.info("Updating database settings")
    # Up maintenance working memory for handling indexing, foreign keys, etc.
    session.execute("SET maintenance_work_mem TO 1000000;")
    logger.info("Committed maintenance_work_mem")

    logger.info("Reading from file:'{}'".format(file_name))
    store = pd.HDFStore(file_name, mode="r")
    keys = keys or store.keys()

    sql_to_hdf = defaultdict(list)
    levels = {}

    logger.info("Determining HDF/SQL table correspondances and levels")
    for key in keys:
        try:
            metadata = store.get_storer(key).attrs.atlas_metadata
            logger.info("Metadata: {}".format(metadata))
        except AttributeError:
            logger.info("Skipping {}".format(key))
            continue

        # Get levels for tables to use for later
        levels[metadata['sql_table_name']] = metadata['levels']

        sql_name = metadata.get("sql_table_name")
        if sql_name:
            sql_to_hdf[sql_name].append(key)
        else:
            logger.warn("No SQL table name found for {}".format(key))

    store.close()

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

    try:
        for sql_table in sql_to_hdf.keys():
            logger.info("Entering copy_to_postgres function")
            rows += copy_to_postgres(session, sql_table, sql_to_hdf, file_name,
                                     levels, chunksize)
    except SQLAlchemyError as exc:
            logger.error(exc)
    finally:
        # Add foreign keys back in after all data loaded to not worry about order
        for fk in db_foreign_keys:
            session.execute(AddConstraint(fk))

    # Set this back to default value
    session.execute("SET maintenance_work_mem TO 259000;")
    session.commit()
    logger.info("Job complete. {} rows copied to db.".format(rows))


def import_data(file_name="./data.h5", engine=None, source_chunksize=10**6,
                dest_chunksize=10**6, keys=None, database="postgres"):
    """Import data from a data.h5 (i.e. HDF) file into the SQL DB. This
    needs to be run from within the flask app context in order to be able to
    access the db engine currently in use.

    In the HDF store, we expect to find one table per SQL table, with specific
    attributes:

        - sql_table_name: tells us which table to write to. If it doesn't
        exist, we skip reading this table.
        - product_level: modifies the "level" column, for cases when multiple
        levels of data (e.g. 2digit and 4digit) get loaded into a single table

    In addition, anything under the /classifications/ path in the HDF store
    gets treated specially as a classification, and gets run through the
    classification_to_pandas() function.
    """

    if database == "postgres":
        # No engine defined as a kwarg for postgres
        import_data_postgres(file_name=file_name,
                             chunksize=min(source_chunksize, dest_chunksize),
                             keys=keys)

    elif database == "sqlite":
        import_data_sqlite(file_name=file_name, engine=engine,
                           source_chunksize=source_chunksize,
                           dest_chunksize=dest_chunksize,
                           keys=keys)
