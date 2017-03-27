from . import core

def convert_classification(df, optional_fields=["name_es", "name_short_en",
                                                "name_short_es",
                                                "description_en",
                                                "description_es"]):
    """Convert a classification from the format it comes in the classification
    file (which is the format from the 'classifications' github repository)
    into the format that the flask apps use. Mostly just a thing for dropping
    unneeded columns and renaming existing ones.

    The optional_fields allows you to specify which fields should be considered
    optional, i.e. it'll still work if this field doesn't exist in the
    classification, like the description fields for example.
    """

    # Pull in some related fields and change names appropriately
    new_df = df[["index", "code", "name", "level", "parent_id"]]
    new_df = new_df.rename(columns={
        "index": "id",
        "name": "name_en"
    })

    for field in optional_fields:
        if field in df:
            new_df[field] = df[field]

    return new_df


def import_data(file_name="./data.h5", engine=core.db.engine):
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
    convert_classification() function.
    """

    # Keeping this import inlined to avoid a dependency unless needed
    import pandas as pd

    print("Reading from file:'{}'".format(file_name))
    store = pd.HDFStore(file_name)

    for key in store.keys():
        print("-----------------------------------")

        metadata = store.get_storer(key).attrs.atlas_metadata
        print(metadata)

        table_name = metadata.get("sql_table_name", None)
        print(key, table_name)

        if table_name is None:
            print("Skipping {}".format(key))
            continue

        try:
            table = store[key]

            if key.startswith("/classifications/"):
                table = convert_classification(table)

            if "levels" in metadata:
                for level, level_value in metadata["levels"].items():
                    table[level+"_level"] = level_value

            table.to_sql(table_name, engine, index=False,
                         chunksize=10000, if_exists="append")

        except SQLAlchemyError as exc:
            print(exc)
