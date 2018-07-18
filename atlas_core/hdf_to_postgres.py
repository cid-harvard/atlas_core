import logging
from atlas_core import db
from multiprocessing import Pool
from sqlalchemy.exc import SQLAlchemyError
from pandas_to_postgres import (
    HDFTableCopy,
    SmallHDFTableCopy,
    BigHDFTableCopy,
    cast_pandas,
    hdf_metadata,
    copy_worker,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d - %(name)s - %(levelname)s %(message)s",
    datefmt="%Y-%m-%d,%H:%M:%S",
)

logger = logging.getLogger("hdf_to_postgres")


e = db.engine


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


def classification_to_pandas(
    df,
    optional_fields=[
        "name_es",
        "name_short_en",
        "name_short_es",
        "description_en",
        "description_es",
        "is_trusted",
        "in_rankings",
    ],
    **kwargs,
):
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
    new_df = new_df.rename(columns={"index": "id", "name": "name_en"})

    for field in optional_fields:
        if field in df:
            new_df[field] = df[field]

    return new_df


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
    engine_args=[db.engine.url],
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
    args = zip(
        tables,
        [engine_args] * len(tables),
        [engine_kwargs] * len(tables),
        [maintenance_work_mem] * len(tables),
        [add_level_metadata, cast_pandas] * len(tables),
    )
    try:
        p = Pool(processes)
        result = p.starmap_async(copy_worker, args, chunksize=1)
    finally:
        del tables
        p.close()
        p.join()
    if not result.successful():
        # If there's an exception, throw it, but we don't care about the
        # results
        result.get()


if __name__ == "__main__":
    hdf_to_postgres(
        file_name="./data.h5",
        keys=None,
        processes=4,
        engine_args=[db.engine.url],
        maintenance_work_mem="1GB",
        hdf_chunksize=10 ** 7,
        csv_chunksize=10 ** 6,
    )
