from atlas_core import db
from pandas import DataFrame
from multiprocessing import Pool
from sqlalchemy.exc import SQLAlchemyError
from pandas_to_postgres import (
    HDFTableCopy,
    SmallHDFTableCopy,
    BigHDFTableCopy,
    HDFMetadata,
    logger,
    cast_pandas,
)

e = db.engine


def add_level_metadata(
    df: DataFrame, copy_obj: HDFTableCopy, hdf_table: str, **kwargs
) -> DataFrame:
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

    hdf_levels = copy_obj.levels.get(hdf_table)

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


def create_table_objects(hdf_meta, csv_chunksize=10 ** 6):
    classifications = []
    partners = []
    other = []

    for sql_table, hdf_tables in hdf_meta.sql_to_hdf.items():
        if any("classifications/" in table for table in hdf_tables):
            classifications.append(
                SmallHDFTableCopy(
                    hdf_tables,
                    hdf_meta,
                    defer_sql_objs=True,
                    sql_table=sql_table,
                    csv_chunksize=csv_chunksize,
                )
            )
        elif any("partner" in table for table in hdf_tables):
            partners.append(
                BigHDFTableCopy(
                    hdf_tables,
                    hdf_meta,
                    defer_sql_objs=True,
                    sql_table=sql_table,
                    csv_chunksize=csv_chunksize,
                )
            )
        else:
            other.append(
                HDFTableCopy(
                    hdf_tables,
                    hdf_meta,
                    defer_sql_objs=True,
                    sql_table=sql_table,
                    csv_chunksize=csv_chunksize,
                )
            )

    # Return the objects sorted classifications, then partner, then other
    return classifications, partners + other


def copy_worker(
    copy_obj, multiprocess=True, data_formatters=[cast_pandas, add_level_metadata]
):
    e.dispose()
    with e.connect() as conn:
        conn.execution_options(autocommit=True)
        conn.execute("SET maintenance_work_mem TO 1000000;")

        if multiprocess:
            table_obj = db.metadata.tables[copy_obj.sql_table]
            copy_obj.instantiate_sql_objs(conn, table_obj)

        copy_obj.copy(data_formatters=data_formatters)


def hdf_to_postgres(
    file_name="./data.h5", keys=None, hdf_chunksize=10 ** 7, csv_chunksize=10 ** 6
):
    hdf = HDFMetadata(file_name, keys, hdf_chunksize)
    classifications, tables = create_table_objects(hdf, csv_chunksize=csv_chunksize)

    with e.connect() as conn:
        conn.execution_options(autocommit=True)
        conn.execute("SET maintenance_work_mem TO 1000000;")

        for ct in classifications:
            table_obj = db.metadata.tables[ct.sql_table]
            ct.instantiate_sql_objs(conn, table_obj)
            # Order of data_formatters list matters here
            copy_worker(
                ct,
                multiprocess=False,
                data_formatters=[classification_to_pandas, cast_pandas],
            )

    try:
        p = Pool(4)
        p.imap_unordered(copy_worker, tables, chunksize=1)
        # p.map(copy_worker, tables, chunksize=1)
    except Exception as ex:
        logger.exception(ex)
    finally:
        del tables
        del hdf
        p.close()
        p.join()


if __name__ == "__main__":
    hdf_to_postgres(
        file_name="./data.h5", keys=None, hdf_chunksize=10 ** 7, csv_chunksize=10 ** 6
    )
