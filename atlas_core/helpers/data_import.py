def translate_columns(df, translation_table):
    """Take a dataframe, filter only the columns we want, rename them, drop all
    other columns.

    :param df: pandas dataframe
    :param translation_table: dict[column_name_before -> column_name_after]
    """
    return df[list(translation_table.keys())]\
        .rename(columns=translation_table)
