"""Import from an ingested .hdf file to an sql database."""

from collections import defaultdict
from io import StringIO

from sqlalchemy.schema import AddConstraint, DropConstraint
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import reflection

from atlas_core import db


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


def import_data_postgres(file_name="./data.h5", chunksize=10**6, keys=None):
    # Keeping this import inlined to avoid a dependency unless needed
    import pandas as pd
    import numpy as np

    session = db.session

    print("Updating database settings")
    # Up maintenance working memory for handling indexing, foreign keys, etc.
    session.execute("SET maintenance_work_mem TO '1GB';")
    session.commit()
    print("Committed maintenance_work_mem")

    print("Reading from file:'{}'".format(file_name))
    store = pd.HDFStore(file_name, mode="r")
    keys = keys or store.keys()

    sql_to_hdf = defaultdict(list)
    levels = {}

    print("Determining HDF/SQL table correspondances and levels")
    for key in keys:
        try:
            metadata = store.get_storer(key).attrs.atlas_metadata
            print("Metadata: {}".format(metadata))
        except AttributeError:
            print("Skipping {}".format(key))
            continue

        # Get levels for tables to use for later
        levels[metadata['sql_table_name']] = metadata['levels']

        sql_name = metadata.get("sql_table_name")
        if sql_name:
            sql_to_hdf[sql_name].append(key)

    # Drop all foreign keys first to allow for dropping PKs after
    print("Dropping foreign keys for all tables")
    insp = reflection.Inspector.from_engine(db.engine)
    db_foreign_keys = []
    for sql_table in insp.get_table_names():
        fks = db.metadata.tables[sql_table].foreign_key_constraints
        for fk in fks:
            session.execute(DropConstraint(fk))
        db_foreign_keys += fks

    rows = 0

    for sql_table in sql_to_hdf.keys():
        # Drop PK for table
        print("Dropping {} primary key".format(sql_table))
        pk = db.metadata.tables[sql_table].primary_key
        session.execute(DropConstraint(pk))

        # Truncate SQL table
        print("Truncating {}".format(sql_table))
        session.execute('TRUNCATE TABLE {};'.format(sql_table))

        # Copy all HDF tables related to SQL table
        hdf_tables = sql_to_hdf.get(sql_table)

        if hdf_tables is None:
            print("No HDF table found for SQL table {}".format(sql_table))
            continue

        for hdf_table in hdf_tables:
            print("Reading HDF table {}".format(hdf_table))
            df = store[hdf_table]
            rows += len(df)

            # Handle classifications differently
            if hdf_table.startswith("/classifications/"):
                print("Formatting classification {}".format(hdf_table))
                df = classification_to_pandas(df)
                columns = df.columns

            # Add in level fields
            if levels.get(sql_table):
                print("Updating {} level fields".format(hdf_table))
                for entity, level_value in levels.get(sql_table).items():
                    df[entity+"_level"] = level_value

            try:
                # Break large dataframes into managable chunks
                if len(df) > chunksize:
                    n_arrays = (len(df) // chunksize) + 1
                    split_dfs = np.array_split(df, n_arrays)
                    del df

                    for i, split_df in enumerate(split_dfs):
                        print("Creating CSV in memory for {} chunk {} of {}"
                              .format(hdf_table, i + 1, n_arrays))
                        fo = create_file_object(split_df)
                        del split_df

                        print("Inserting {} chunk {} of {}"
                              .format(hdf_table, i + 1, n_arrays))
                        copy_to_database(session, sql_table, columns, fo)
                        del fo

                else:
                    print("Creating CSV in memory for {}".format(hdf_table))
                    fo = create_file_object(df)
                    del df

                    print("Inserting {} data".format(hdf_table))
                    copy_to_database(session, sql_table, columns, fo)
                    del fo

            except SQLAlchemyError as exc:
                print(exc)

        # Adding keys back to table
        print("Recreating {} primary key".format(sql_table))
        session.execute(AddConstraint(pk))

    # Add foreign keys back in after all data loaded to not worry about order
    for fk in db_foreign_keys:
        session.execute(AddConstraint(fk))

    # Set this back to default value
    session.execute("SET maintenance_work_mem TO '259MB';")
    session.commit()
    print("Job complete. {} rows copied to db.".format(rows))


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
