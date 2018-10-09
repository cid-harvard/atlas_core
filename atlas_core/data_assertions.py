import numpy as np
import pandas as pd

from functools import reduce
from operator import mul


def num_missing(series):
    return series.isnull().sum()


def assert_none_missing(series):
    missing_count = num_missing(series)
    assert missing_count == 0


def assert_is_zeropadded_string(series):

    # Must be a string
    assert series.dtype in [np.object, np.str]

    # All entries in this column must be same length
    assert series.str.len().nunique() == 1


def matching_stats(series, classification_level):

    num_rows = series.shape[0]

    unique = pd.Series(series.unique())
    num_unique = unique.shape[0]

    classification_unique = classification_level.code

    rows_not_in_classification = ~series.isin(classification_unique)
    percent_rows_not_in_classification = 100.0 * (
        rows_not_in_classification.sum() / num_rows
    )

    unique_not_in_classification = ~unique.isin(classification_unique)
    percent_unique_not_in_classification = 100.0 * (
        unique_not_in_classification.sum() / num_unique
    )

    codes_missing = unique[unique_not_in_classification]
    codes_unused = classification_unique[~classification_unique.isin(series)]

    return (
        percent_rows_not_in_classification,
        percent_unique_not_in_classification,
        codes_missing,
        codes_unused,
    )


def assert_matches_classification_level(series, classification_level):
    assert matching_stats(series, classification_level)[0] == 0


def fillin(df, entities):
    """STATA style "fillin", make sure all permutations of entities in the
    index are in the dataset."""
    df = df.set_index(entities)
    return df.reindex(pd.MultiIndex.from_product(df.index.levels, names=df.index.names))


def assert_rectangularized(df, entities):
    """Check if all possibilities of all entities have been used"""
    unique_entities = [df[entity].size for entity in entities]
    uniques_multiplied = reduce(mul, unique_entities)

    # In a rectangularized matrix, the number of unique entities multiplied
    # should give you the number of rows
    assert uniques_multiplied == df.shape[0]


def assert_entities_not_duplicated(df, entities):
    assert df.duplicated(subset=entities).any() == False


# Thoughts:
# - Dataset  python class?
# - Layered approach: pandas -> assertions -> classification specific reckoner assertions -> reckoner
# - Write your own intervention.py or whatever that runs and edits the dataframe right before assertions run
# - Test case output - quality metrics?
# - pytest_generate_assertions is key

# Somehow make sure that nulls haven't been turned into zeroes. Maybe count the number of zero fields?
# PCIs should add up to zero?!
