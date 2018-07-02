import pandas as pd

from collections import defaultdict
from io import StringIO
import logging

# Add new formatted log handler for data_import
logging.basicConfig(
    level=logging.DEBUG,
    format="%(levelname)s %(asctime)s.%(msecs)03d %(message)s",
    datefmt="%Y-%m-%d,%H:%M:%S"
)

logger = logging.getLogger('data_import')


def create_file_object(df):
    '''
    Writes pandas dataframe to an in-memory StringIO file object. Adapted from
    https://gist.github.com/mangecoeur/1fbd63d4758c2ba0c470#gistcomment-2086007
    '''
    file_object = StringIO()
    df.to_csv(file_object, index=False)
    file_object.seek(0)
    return file_object


def cast_pandas(df, table_obj):
    '''
    Pandas does not handle null values in integer or boolean fields out of the
    box, so cast fields that should be these types in the database to object
    fields and change np.nan to None

    Parameters
    ----------
    df: pandas dataframe
        data frame with fields that are desired to be int or bool as float with
        np.nan that should correspond to None

    table_obj: SQLAlchemy model instance
        destination table object with field names corresponding to those in df

    Returns
    -------
    df: pandas dataframe
        dataframe with fields that correspond to Postgres int, bigint, and bool
        fields changed to objects with None values for null
    '''

    for col in table_obj.columns:
        if str(col.type) in ['INTEGER', 'BIGINT']:
            df[col.name] = df[col.name].apply(
                # np.nan are not comparable, so use str value
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


def hdf_metadata(file_name, keys):
    '''
    Returns information on SQL-HDF table pairs (one-to-many) and classification
    levels of each HDF table (many-to-one)

    Parameters
    ----------
    file_name: str
        path to HDF file
    keys: iterable
        set of keys in HDF to limit load to

    Returns
    -------
    sql_to_hdf: dict
        each sql table name key corresponds to a list of HDF keys that belong
        in the table (such as different HDF tables for different product digit
        levels)
    levels: dict
        for each key of an HDF table, provides data on the associated levels
        for classification fields (such as 2digit, country)
    '''

    store = pd.HDFStore(file_name, mode="r")
    keys = keys or store.keys()

    sql_to_hdf = defaultdict(list)
    levels = {}

    for key in keys:
        try:
            metadata = store.get_storer(key).attrs.atlas_metadata
            logger.info("Metadata: %s", metadata)
        except AttributeError:
            logger.info("Attribute Error: Skipping %s", key)
            continue

        # Get levels for tables to use for later
        levels[key] = metadata['levels']

        sql_name = metadata.get("sql_table_name")
        if sql_name:
            sql_to_hdf[sql_name].append(key)
        else:
            logger.warn("No SQL table name found for %s", key)

    store.close()

    return sql_to_hdf, levels
