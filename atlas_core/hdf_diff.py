import sys
import pandas as pd
from clint.textui import puts, indent, colored


class HDFInfo(object):

    unique_keys = set()
    unique_columns = {}

    def __init__(self, name):
        self.name = name
        self.hdf = pd.HDFStore(self.name)
        self.keys = {key for key in self.hdf.keys() if "meta" not in key}


def key_check(file_1, file_2):
    "Checks if files have same keys or prints differences. Returns common keys"

    if file_1.keys == file_2.keys:
        puts(colored.green("Files have same keys"))
        common_keys = file_1.keys
    else:
        puts(colored.red("Files contain different keys"))
        file_1.unique_keys = file_1.keys - file_2.keys
        file_2.unique_keys = file_2.keys - file_1.keys
        common_keys = file_2.keys & file_1.keys

        def key_diff(file):
            with indent(4, quote=colored.cyan("> ")):
                puts(colored.yellow(f"Only in {file.name}:"))
                with indent(4, quote=colored.cyan("- ")):
                    for key in file.unique_keys:
                        puts(colored.yellow(key))

        if file_1.unique_keys:
            key_diff(file_1)
        if file_1.unique_keys:
            key_diff(file_2)

    with indent(4, quote=colored.cyan("> ")):
        puts(colored.green("Common keys:"))
        with indent(4, quote=colored.green("- ")):
            for key in common_keys:
                puts(colored.green(key))

    return common_keys


def compare_tables(common_keys, file_1, file_2):
    puts(colored.cyan("\n\n** COMPARING COMMON TABLES **"))
    for key in common_keys:
        puts(colored.cyan(f"\nComparing {key}:"))
        with indent(4, quote=colored.cyan("> ")):
            column_checks(key, file_1, file_2)
            row_count_checks(key, file_1, file_2)


def column_checks(table, file_1, file_2, list_exact_common=False):
    columns_1 = set(file_1.hdf[table].columns)
    columns_2 = set(file_2.hdf[table].columns)

    if columns_1 == columns_2:
        puts(colored.green("Tables have same columns"))

        # Return early if we don't need to list the common columns if exact
        if not list_exact_common:
            return columns_1

    else:
        puts(colored.red("Tables contain different keys"))
        file_1.unique_columns[table] = file_1.keys - file_2.keys
        file_2.unique_columns[table] = file_2.keys - file_1.keys
        common_columns = file_2.keys & file_1.keys

        def col_diff(file):
            with indent(4, quote=colored.cyan("> ")):
                puts(colored.yellow(f"Only in {file.name}:"))
                with indent(4, quote=colored.cyan("- ")):
                    for col in file.unique_columns[table]:
                        puts(colored.yellow(col))

        if file_1.unique_columns[table]:
            col_diff(file_1)
        if file_2.unique_columns[table]:
            col_diff(file_2)

    with indent(4, quote=colored.cyan("> ")):
        puts(colored.cyan(f"Common columns:"))
        with indent(4, quote=colored.cyan("- ")):
            for col in common_columns:
                puts(colored.cyan(col))

    return common_columns


def row_count_checks(table, file_1, file_2):
    rows_1 = len(file_1.hdf[table].index)
    rows_2 = len(file_2.hdf[table].index)
    diff = abs(rows_1 - rows_2)

    if rows_1 == rows_2:
        puts(colored.green("Tables have same row counts"))
    else:
        puts(colored.yellow("Tables have different row counts:"))
        with indent(4, quote=colored.yellow("> ")):
            puts(colored.yellow(f"{file_1.name}: {rows_1:,} rows"))
            puts(colored.yellow(f"{file_2.name}: {rows_2:,} rows"))
            puts(colored.yellow(f"Difference: {diff:,} rows"))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python hdf_diff.py <file_1.h5> <file_2.h5>")
        sys.exit(1)

    files = (HDFInfo(sys.argv[1]), HDFInfo(sys.argv[2]))
    common_keys = key_check(*files)
    compare_tables(common_keys, *files)
