"""Import from an ingested .hdf file to an sql database."""
from sqlalchemy.exc import SQLAlchemyError

from load_data import load_postgres, classification_to_pandas


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
        load_postgres(file_name=file_name,
                      chunksize=min(source_chunksize, dest_chunksize),
                      keys=keys, commit_every=True)

    elif database == "sqlite":
        import_data_sqlite(file_name=file_name, engine=engine,
                           source_chunksize=source_chunksize,
                           dest_chunksize=dest_chunksize,
                           keys=keys)
