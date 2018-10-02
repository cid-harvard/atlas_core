"""Import from an ingested .hdf file to an sql database."""
from sqlalchemy.exc import SQLAlchemyError
from .helpers.classifications import classification_to_pandas


def import_data_sqlite(
    file_name="./data.h5",
    engine=None,
    keys=None,
    source_chunksize=10 ** 6,
    dest_chunksize=10 ** 6,
):
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
                df.to_sql(
                    table_name,
                    engine,
                    index=False,
                    chunksize=dest_chunksize,
                    if_exists="append",
                )

            else:
                # If it's a timeseries data table, load it in chunks to not
                # exhaust memory all at once
                iterator = pd.read_hdf(
                    file_name, key=key, chunksize=source_chunksize, iterator=True
                )

                for i, df in enumerate(iterator):
                    print(i * source_chunksize)

                    # Add in level fields
                    if "levels" in metadata:
                        for entity, level_value in metadata["levels"].items():
                            df[entity + "_level"] = level_value

                    df.to_sql(
                        table_name,
                        engine,
                        index=False,
                        chunksize=dest_chunksize,
                        if_exists="append",
                    )

                    # Hint that this object should be garbage collected
                    del df

        except SQLAlchemyError as exc:
            print(exc)


def import_data(
    file_name="./data.h5",
    engine=None,
    source_chunksize=10 ** 7,
    dest_chunksize=10 ** 6,
    keys=None,
    database="postgres",
    processes=4,
    new_db_name=None,
):
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

    Postgres-specific:
    ------------------
    The Postgres implementation needs an engine with a URL pointing to to an
    already existing database. This is because you can't connect to a
    non-existent db in postges, but we need to connect to the server to create
    the new DB. The new database name is derived from the data version in the
    data_info table of the HDF file, or can be overridden with `new_db_name`.
    The URL from the given engine is then modified to point to the newly
    created database name.

    It is worth noting that this does use the atlas_core.db object to connect
    to to create the new database as well as use its metadata to create the
    database structures in the destination db.
    """

    if database == "postgres":
        from .hdf_to_postgres import multiload

        multiload(
            file_name=file_name,
            engine=engine,
            new_db_name=new_db_name,
            hdf_chunksize=source_chunksize,
            csv_chunksize=dest_chunksize,
            keys=keys,
            maintenance_work_mem="1GB",
            processes=processes,
        )
    elif database == "sqlite":
        import_data_sqlite(file_name, engine, keys, source_chunksize, dest_chunksize)
    else:
        raise ValueError(
            f"Database must be one of 'postgres' or 'sqlite', you gave {database}"
        )
