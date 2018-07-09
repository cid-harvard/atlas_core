from .utilities import (
    create_file_object,
    df_generator,
    logger,
    hdf_metadata,
    classification_to_pandas,
    cast_pandas,
    add_level_metadata,
)

from atlas_core import db

import pandas as pd
from collections import defaultdict
from sqlalchemy.schema import AddConstraint, DropConstraint
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker


class PGTable(object):

    rows = 0
    columns = None

    def __init__(self, database, sql_table, hdf_tables, hdf, csv_chunksize=10 ** 6):
        self.db = database
        self.make_session()

        self.sql_table = sql_table
        self.hdf_tables = hdf_tables
        self.table_obj = self.db.metadata.tables[sql_table]
        self.primary_key = self.table_obj.primary_key
        self.foreign_keys = self.table_obj.foreign_key_constraints
        self.csv_chunksize = csv_chunksize

        # Info from the HDFMetadata object
        self.levels = hdf.levels
        self.file_name = hdf.file_name
        self.hdf_chunksize = hdf.chunksize

    def make_session(self):
        self.db.engine.dispose()
        self.session = scoped_session(sessionmaker(self.db.engine))()
        self.session.execute("SET maintenance_work_mem TO 1000000;")

    def commit(self):
        self.session.commit()
        self.session.execute("SET maintenance_work_mem TO 1000000;")

    def rollback(self):
        self.session.rollback()
        self.session.execute("SET maintenance_work_mem TO 1000000;")

    def close(self):
        self.session.commit()
        self.session.remove()
        del self.session

    def drop_pk(self):
        logger.info(f"Dropping {self.sql_table} primary key")
        self.session.execute(DropConstraint(self.primary_key))

    def create_pk(self):
        logger.info(f"Creating {self.sql_table} primary key")
        self.session.execute(AddConstraint(self.primary_key))

    def drop_fks(self):
        self.commit()
        for fk in self.foreign_keys:
            try:
                logger.info(f"Dropping foreign key {fk.name}")
                self.session.execute(DropConstraint(fk))
                self.commit()
            except SQLAlchemyError:
                logger.warn(f"Foreign key {fk.name} not found")
                self.rollback()

    def create_fks(self):
        self.commit()
        for fk in self.foreign_keys:
            try:
                logger.info(f"Creating foreign key {fk.name}")
                self.session.execute(AddConstraint(fk))
                self.commit()
            except SQLAlchemyError:
                logger.warn(f"Error creating foreign key {fk.name}")
                self.rollback()

    def truncate(self):
        logger.info(f"Truncating {self.sql_table}")
        self.session.execute(f"TRUNCATE TABLE {self.sql_table};")

    def copy_from_file(self, file_object):
        cur = self.session.connection().connection.cursor()
        cols = ", ".join([f"{col}" for col in self.columns])
        sql = f"COPY {self.table} ({cols}) FROM STDIN WITH CSV HEADER FREEZE"
        cur.copy_expert(sql=sql, file=file_object)

    def copy_table(self):
        # Assumes foreign keys already dropped for now
        self.drop_pk()
        self.truncate()
        self.hdf_to_pg()
        self.create_pk()
        self.create_fks()

    def hdf_to_pg(self):
        if self.hdf_tables is None:
            logger.warn(f"No HDF table found for SQL table {self.sql_table}")
            return

        for hdf_table in self.hdf_tables:
            logger.info(f"*** {hdf_table} ***")
            hdf_levels = self.levels.get(hdf_table)

            logger.info("Reading HDF table")
            df = pd.read_hdf(self.file_name, key=hdf_table)
            self.rows += len(df)

            # Handle NaN --> None type casting and adding const level data
            df = cast_pandas(df, self.table_obj)
            df = add_level_metadata(df, hdf_levels)

            if not self.columns:
                self.columns = df.columns

            logger.info("Creating generator for chunking dataframe")
            for chunk in df_generator(df, self.csv_chunksize):

                logger.info("Creating CSV in memory")
                fo = create_file_object(chunk)

                logger.info("Copying chunk to database")
                self.copy_from_file(fo)
                del fo
            del df
        logger.info(f"All chunks copied ({self.rows} rows)")


class PGClassificationTable(PGTable):
    def hdf_to_pg(self):
        if self.hdf_tables is None:
            logger.warn("No HDF table found for SQL table {self.sql_table}")
            return

        for hdf_table in self.hdf_tables:
            logger.info(f"*** {hdf_table} ***")
            logger.info("Reading HDF table")
            df = pd.read_hdf(self.file_name, key=hdf_table)
            self.rows += len(df)

            logger.info("Formatting classification")
            df = classification_to_pandas(df)
            df = cast_pandas(df, self.table_obj)

            if not self.columns:
                self.columns = df.columns

            logger.info("Creating CSV in memory")
            fo = create_file_object(df)

            logger.info("Copying table to database")
            self.copy_from_file(fo)
            del df
            del fo
        logger.info(f"All chunks copied ({self.rows} rows)")


class PGPartnerTable(PGTable):
    def hdf_to_pg(self):
        if self.hdf_tables is None:
            logger.warn(f"No HDF table found for SQL table {self.sql_table}")
            return

        for hdf_table in self.hdf_tables:
            logger.info(f"*** {hdf_table} ***")
            hdf_levels = self.levels.get(hdf_table)

            with pd.HDFStore(self.file_name) as store:
                nrows = store.get_storer(hdf_table).nrows

            self.rows += nrows
            if nrows % self.hdf_chunksize:
                n_chunks = (nrows // self.hdf_chunksize) + 1
            else:
                n_chunks = nrows // self.hdf_chunksize

            start = 0

            for i in range(n_chunks):
                logger.info(f"*** HDF chunk {i + 1} of {n_chunks} ***")
                logger.info("Reading HDF table")
                stop = min(start + self.hdf_chunksize, nrows)
                df = pd.read_hdf(self.file_name, key=hdf_table, start=start, stop=stop)

                start += self.hdf_chunksize

                # Handle NaN --> None type casting and adding const level data
                df = cast_pandas(df, self.table_obj)
                df = add_level_metadata(df, hdf_levels)

                logger.info("Creating generator for chunking dataframe")
                for chunk in df_generator(df, self.csv_chunksize):
                    logger.info("Creating CSV in memory")
                    fo = create_file_object(chunk)

                    logger.info("Copying chunk to database")
                    self.copy_from_file(fo)
                    del fo
                del df
        logger.info(f"All chunks copied ({self.rows} rows)")


class HDFMetadata(object):

    self.sql_to_hdf = defaultdict(list)
    self.levels = {}

    def __init__(self, file_name="./data.h5", keys=None, chunksize=10 ** 7):
        self.file_name = file_name
        self.keys = keys or self.store.keys()
        self.chunksize = chunksize

        store = pd.HDFStore(self.file_name, mode="r")
        for key in self.keys:
            try:
                metadata = store.get_storer(key).attrs.atlas_metadata
                logger.info(f"Metadata: {metadata}")
            except AttributeError:
                logger.info(f"Attribute Error: Skipping {key}")
                continue

            self.levels[key] = metadata['levels']

            sql_table = metadata.get("sql_table_name")
            if sql_table:
                self.sql_to_hdf[sql_table].append(key)
            else:
                logger.warn(f"No SQL table name found for {key}")
        store.close()


def create_table_objects(hdf):
    classifications = []
    partners = []
    other = []

    for sql_table, hdf_tables in hdf.sql_to_hdf.items():
        if any("classifications/" in table for table in hdf_tables):
            classifications.append(PGClassificationTable(db, sql_table, hdf_tables, hdf, csv_chunksize))
        elif any("partner" in table for table in hdf_tables):
            partners.append(PGPartnerTable(db, sql_table, hdf_tables, hdf, csv_chunksize))
        else:
            other.append(PGTable(db, sql_table, hdf_tables, hdf, csv_chunksize))

    # Return the objects sorted classifications, then partner, then other
    return classifications + partners + other


def hdf_to_postgres(
    file_name="./data.h5", keys=None, hdf_chunksize=10 ** 7, csv_chunksize=10 ** 6
):

    hdf = HDFMetadata(file_name, keys, hdf_chunksize)
    tables = create_table_objects(hdf, csv_chunksize)

    # Drop all FKs at beginning to not have conflicts
    for table in tables:
        table.drop_fks()

    for table in tables:
        table.
