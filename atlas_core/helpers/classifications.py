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
        "reported_serv",
        "reported_serv_recent",
        "iso2",
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
