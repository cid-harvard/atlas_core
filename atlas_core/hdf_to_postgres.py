from atlas_core import db
from .helpers.classifications import classification_to_pandas
from multiprocessing import Pool
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
import string
from pandas_to_postgres import (
    HDFTableCopy,
    SmallHDFTableCopy,
    BigHDFTableCopy,
    cast_pandas,
    hdf_metadata,
    copy_worker,
    get_logger,
)

logger = get_logger("hdf_to_postgres")


def add_level_metadata(df, copy_obj, hdf_table, **kwargs):
    """
    Updates dataframe fields for constant "_level" fields

    Parameters
    ----------
    df: pandas DataFrame
    hdf_levels: dict of level:value fields that are constant for the entire dataframe

    Returns
    ------
    df: pandas DataFrame with level fields added
    """

    hdf_levels = copy_obj.hdf_metadata["levels"].get(hdf_table)

    if hdf_levels:
        logger.info("Adding level metadata values")
        for entity, level_value in hdf_levels.items():
            df[entity + "_level"] = level_value

    return df


def create_table_objects(
    file_name, sql_to_hdf, csv_chunksize=10 ** 6, hdf_chunksize=10 ** 7, hdf_meta=None
):
    classifications = []
    partners = []
    other = []

    for sql_table, hdf_tables in sql_to_hdf.items():
        if any("classifications/" in table for table in hdf_tables):
            classifications.append(
                SmallHDFTableCopy(
                    file_name,
                    hdf_tables,
                    defer_sql_objs=True,
                    sql_table=sql_table,
                    csv_chunksize=csv_chunksize,
                    hdf_chunksize=hdf_chunksize,
                    hdf_metadata=hdf_meta,
                )
            )
        elif any("partner" in table for table in hdf_tables):
            partners.append(
                BigHDFTableCopy(
                    file_name,
                    hdf_tables,
                    defer_sql_objs=True,
                    sql_table=sql_table,
                    csv_chunksize=csv_chunksize,
                    hdf_chunksize=hdf_chunksize,
                    hdf_metadata=hdf_meta,
                )
            )
        else:
            other.append(
                HDFTableCopy(
                    file_name,
                    hdf_tables,
                    defer_sql_objs=True,
                    sql_table=sql_table,
                    csv_chunksize=csv_chunksize,
                    hdf_chunksize=hdf_chunksize,
                    hdf_metadata=hdf_meta,
                )
            )

    # Return the objects sorted classifications, then partner, then other
    return classifications, partners + other


def hdf_to_postgres(
    file_name="./data.h5",
    keys=[],
    processes=4,
    engine_args=[],
    engine_kwargs={},
    maintenance_work_mem="1GB",
    hdf_chunksize: int = 10 ** 7,
    csv_chunksize: int = 10 ** 6,
):

    sql_to_hdf, metadata_vars = hdf_metadata(
        file_name, keys=keys, metadata_attr="atlas_metadata", metadata_keys=["levels"]
    )

    classifications, tables = create_table_objects(
        file_name,
        sql_to_hdf,
        csv_chunksize=csv_chunksize,
        hdf_chunksize=hdf_chunksize,
        hdf_meta=metadata_vars,
    )

    # Copy classifications first, not multiprocessed
    for class_table in classifications:
        # Order of data_formatters list matters here
        copy_worker(
            class_table,
            engine_args,
            engine_kwargs,
            maintenance_work_mem=maintenance_work_mem,
            data_formatters=[classification_to_pandas, cast_pandas],
        )

    # Use multiprocessing for the larger tables once classifications are complete
    pool_args = zip(
        tables,
        [engine_args] * len(tables),
        [engine_kwargs] * len(tables),
        [maintenance_work_mem] * len(tables),
        [[add_level_metadata, cast_pandas]] * len(tables),
    )
    try:
        p = Pool(processes)
        result = p.starmap_async(copy_worker, pool_args, chunksize=1)
    finally:
        del tables
        p.close()
        p.join()
    if not result.successful():
        # If there's an exception, throw it, but we don't care about the
        # results
        result.get()


def coerce_data_version(version):
    """try to coerce a data version string into a valid sql92 database name.
    according to the standard, db identifiers can only have letters,
    underscores and digits:
        https://db.apache.org/derby/docs/10.9/ref/crefsqlj1003454.html"""

    # for name/date-based versions, replace dashes with underscores
    version_new = version.replace("-", "_")

    # for v2.3.1 style version,s replace dots with underscores
    version_new = version_new.replace(".", "_")

    valid_chars = set(string.digits + string.ascii_letters + "_")
    if not all(x in valid_chars for x in version_new):
        raise ValueError(
            "Database names can only contain ASCII letters and "
            "numbers, and the underscore. You provided {} which we "
            "coerced into {}.".format(version, version_new)
        )

    return version_new


def multiload(
    engine,
    file_name="./data.h5",
    new_db_name=None,
    keys=None,
    processes=4,
    maintenance_work_mem="1GB",
    hdf_chunksize=10 ** 7,
    csv_chunksize=10 ** 6,
    data_info_key="data_info",
):

    # Fetch database name from data version
    if new_db_name is None:
        import pandas as pd

        # Get data version from data_info table
        info_df = pd.read_hdf(file_name, key=data_info_key).set_index("key")
        new_db_name = info_df.loc["output_data_version", "value"]

    # Coerce into a valid SQL db name
    new_db_name = coerce_data_version(new_db_name)

    # Try to create pg database. Connect using the default URI because in
    # postgres you can't connect to a DB that doesn't exist yet, so we have to
    # start with one that already exists.
    try:
        conn = engine.connect()
        conn.execute("commit")
        conn.execute(f'CREATE DATABASE "{new_db_name}"')
        logger.info(f"Created database {new_db_name}")
    except SQLAlchemyError:
        logger.info(
            f"Error creating database {new_db_name}. It probably already exists"
        )
    finally:
        conn.close()

    # Make new engine URL with existing URL string, replacing database name
    load_url = make_url(engine.url)
    load_url.database = new_db_name
    load_url = str(load_url)

    # Create empty SQL schema according to sqlalchemy models
    load_engine = create_engine(load_url)
    db.metadata.create_all(load_engine)

    # Load data into schema
    hdf_to_postgres(
        file_name=file_name,
        keys=keys,
        processes=processes,
        engine_args=[load_url],
        maintenance_work_mem=maintenance_work_mem,
        hdf_chunksize=hdf_chunksize,
        csv_chunksize=csv_chunksize,
    )
