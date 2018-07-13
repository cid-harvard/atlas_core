from atlas_core import db
from multiprocessing import Pool
from sqlalchemy.exc import SQLAlchemyError
from pandas_to_postgres.utilities import HDFMetadata, logger
from pandas_to_postgres.copy_hdf import (
    HDFTableCopy,
    ClassificationHDFTableCopy,
    BigHDFTableCopy,
)

e = db.engine


def create_table_objects(hdf_meta, csv_chunksize=10 ** 6):
    classifications = []
    partners = []
    other = []

    for sql_table, hdf_tables in hdf_meta.sql_to_hdf.items():
        if any("classifications/" in table for table in hdf_tables):
            classifications.append(
                ClassificationHDFTableCopy(
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


def copy_worker(copy_obj, multiprocess=True):
    e.dispose()
    with e.connect() as conn:
        conn.execution_options(autocommit=True)
        conn.execute("SET maintenance_work_mem TO 1000000;")

        if multiprocess:
            table_obj = db.metadata.tables[copy_obj.sql_table]
            copy_obj.instantiate_sql_objs(conn, table_obj)

        copy_obj.copy_table()


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
            copy_worker(ct, multiprocess=False)

    try:
        p = Pool(4)
        p.map(copy_worker, tables, chunksize=1)
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
