"""All the stuff related to cleaning up raw datasets, merging them with
classifications, etc."""

from io import StringIO

from clint.textui import puts, indent, colored
import pandas as pd

from . import data_assertions as assertions


def good(msg):
    return puts("[^_^] " + colored.green(msg))


def warn(msg):
    return puts("[ಠ_ಠ] " + colored.yellow(msg))


def bad(msg):
    return puts("[ಠ益ಠ] " + colored.red(msg))


def indented():
    return indent(4, quote=colored.cyan("> "))


def merge_ids_from_codes(df, df_merge_on, classification, classification_column):
    """Merge a classification id column to a table, given the code field, and
    rename things nicely."""
    code_to_id = classification.reset_index()[["code", "index"]].set_index("code")
    code_to_id.columns = [classification_column]
    return df.merge(code_to_id, left_on=df_merge_on, right_index=True, how="left")


def process_dataset(dataset):

    puts("=" * 80)
    good("Processing a new dataset!")

    # Read dataset and fix up columns
    df = dataset["read_function"]()
    df = df[list(dataset["field_mapping"].keys())]
    df = df.rename(columns=dataset["field_mapping"])

    if "hook_pre_merge" in dataset:
        df = dataset["hook_pre_merge"](df)

    puts("Dataset overview:")
    with indented():
        infostr = StringIO()
        df.info(buf=infostr, memory_usage=True, null_counts=True)
        puts(infostr.getvalue())

    for field in dataset["facet_fields"]:
        try:
            assertions.assert_none_missing(df[field])
        except AssertionError:
            warn(
                "Field '{}' has {} missing values.".format(
                    field, df[field].isnull().sum()
                )
            )

    # Zero-pad digits of n-digit codes
    for field, length in dataset["digit_padding"].items():
        try:
            assertions.assert_is_zeropadded_string(df[field])
        except AssertionError:
            warn("Field '{}' is not padded to {} digits.".format(field, length))
            df[field] = df[field].astype(int).astype(str).str.zfill(length)

    # Make sure the dataset is rectangularized by the facet fields
    try:
        assertions.assert_rectangularized(df, dataset["facet_fields"])
    except AssertionError:
        warn(
            "Dataset is not rectangularized on fields {}".format(
                dataset["facet_fields"]
            )
        )

    try:
        assertions.assert_entities_not_duplicated(df, dataset["facet_fields"])
    except AssertionError:
        bad(
            "Dataset has duplicate rows for entity combination: {}".format(
                dataset["facet_fields"]
            )
        )
        bad(df[df.duplicated(subset=dataset["facet_fields"], keep=False)])

    # Merge in IDs for entity codes
    for field_name, c in dataset["classification_fields"].items():
        classification_table = c["classification"].level(c["level"])

        (
            p_nonmatch_rows,
            p_nonmatch_unique,
            codes_missing,
            codes_unused,
        ) = assertions.matching_stats(df[field_name], classification_table)

        if p_nonmatch_rows > 0:
            bad("Errors when Merging field {}:".format(field_name))
            with indented():
                puts("Percentage of nonmatching rows: {}".format(p_nonmatch_rows))
                puts("Percentage of nonmatching codes: {}".format(p_nonmatch_unique))
                puts(
                    "Codes missing in classification:\n{}".format(
                        codes_missing.reset_index(drop=True)
                    )
                )
                puts("Codes unused:\n{}".format(codes_unused.reset_index(drop=True)))

            bad("Dropping nonmatching rows.")
            df = df[~df[field_name].isin(codes_missing)]

        df = merge_ids_from_codes(
            df, field_name, classification_table, field_name + "_id"
        )

        df[field_name + "_id"] = df[field_name + "_id"].astype(
            pd.CategoricalDtype(categories=classification_table.index.values)
        )

    if "year" in df.columns:
        df["year"] = df["year"].astype(pd.CategoricalDtype(categories=df.year.unique()))

    # Gather each facet dataset (e.g. DY, PY, DPY variables from DPY dataset)
    facet_outputs = {}
    for facet_fields, aggregations in dataset["facets"].items():
        puts("Working on facet: {}".format(facet_fields))
        facet_groupby = df.groupby(facet_fields)

        # Do specified aggregations / groupings for each column
        # like mean, first, min, rank, etc
        agg_outputs = []
        for agg_field, agg_func in aggregations.items():
            with indented():
                puts("Working on: {}".format(agg_field))

            agged_row = agg_func(facet_groupby[[agg_field]])
            agg_outputs.append(agged_row)

        facet = pd.concat(agg_outputs, axis=1)
        facet_outputs[facet_fields] = facet

    # Perform aggregations by classification (e.g. aggregate 4digit products to
    # 2digit and locations to regions, or both, etc)
    clagg_outputs = {}
    for clagg_name, clagg_settings in dataset.get(
        "classification_aggregations", {}
    ).items():

        # Here is the output dataframe we now want to aggregate up
        facet = clagg_settings["facet"]
        base_df = facet_outputs[facet].reset_index()

        # First, find out new higher_level ids, e.g. each product_id entry
        # should be replaced from the 4digit id to its 2digit parent etc.
        for field, agg_level_to in clagg_settings["agg_fields"].items():

            # Infer classification table and level we're aggregating from, from
            # field name
            assert field.endswith("_id")
            classification_name = field[:-3]
            classifications = dataset["classification_fields"]
            classification_table = classifications[classification_name][
                "classification"
            ]
            agg_level_from = classifications[classification_name]["level"]

            # Check agg level to is a valid level
            assert agg_level_to in classification_table.levels
            assert agg_level_from in classification_table.levels

            # Check agg_level to is higher than agg_level_from
            assert (
                classification_table.levels[agg_level_to]
                < classification_table.levels[agg_level_from]
            )

            # Get table that gives us mapping from agg_level_from to agg_level_to
            aggregation_table = classification_table.aggregation_table(
                agg_level_from, agg_level_to
            )

            # Rename column so that when we join the aggregation table we don't
            # get a duplicate column name accidentally
            assert "parent_id" not in facet_outputs[facet].columns
            aggregation_table.columns = ["parent_id"]

            # Convert the new ID field into a categorical type
            aggregation_table.parent_id = aggregation_table.parent_id.astype(
                int
            ).astype(
                "category", values=aggregation_table.parent_id.astype(int).unique()
            )

            # Join aggregation table
            # Drop old field and replace with new aggregation table field
            base_df = (
                base_df.merge(aggregation_table, left_on=field, right_index=True)
                .drop(columns=field)
                .rename(columns={"parent_id": field})
            )

        # Now that we have the new parent ids for every field, perform
        # aggregation
        agg_df = base_df.groupby(facet).agg(clagg_settings["agg_params"])

        # Add it to the list of classification aggregation results!
        clagg_outputs[clagg_name] = agg_df

    facet_outputs["classification_aggregations"] = clagg_outputs

    puts("Done! ヽ(◔◡◔)ﾉ")

    return facet_outputs


# Cleaning notes
# ==============
# [] Merge similar facet data (DY datasets together, etc)
# [] Function to generate other cross-dataset columns: gdp per capita

# [] Save merged facets into hdf5 file
# [] Load merged facet to given model
# [] Move classification merging code into classification class:
# Classification.merge_to_table Classification.merge_index
