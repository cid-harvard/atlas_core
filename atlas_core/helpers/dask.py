import dask.dataframe as dd
from dask.dataframe.utils import make_meta
from dask.delayed import delayed
from itertools import chain
import pandas as pd


def get_stata_meta(file_name, meta_chunksize=10000, *args, **kwargs):
    """Load up first bit of the file for type metadata info. We have to resort
    to using iter() and chunksize trickery because the read_stata function
    doesn't have an "nrows" option."""
    meta_df = next(pd.read_stata(file_name, *args, chunksize=meta_chunksize, **kwargs))
    return make_meta(meta_df)


def read_stata_with_offset(file_name, start_row=0, nrows=None, *args, **kwargs):
    reader = pd.read_stata(file_name, iterator=True, *args, **kwargs)

    # Reset file pointer to start point
    reader._lines_read = start_row

    return reader.read(nrows=nrows)


@delayed(pure=True)
def read_stata_delayed_simple(*args, **kwargs):
    """Swap the chunked version with this to see the difference."""
    return pd.read_stata(*args, **kwargs)


def read_stata_delayed_chunked(file_name, delayed_chunksize=1000000, *args, **kwargs):
    """Return a list of delayed objects, each of which will return a chunk of
    the given stata file."""

    # Get number of rows
    reader = pd.read_stata(file_name, iterator=True, *args, **kwargs)
    num_rows = reader.nobs
    reader.close()

    # Delayed stata chunk reader
    delayed_read = delayed(read_stata_with_offset, pure=True)

    delayeds = []
    for chunk_offset in range(0, num_rows, delayed_chunksize):

        chunk_task = delayed_read(
            file_name, start_row=chunk_offset, nrows=delayed_chunksize, *args, **kwargs
        )

        delayeds.append(chunk_task)

    return delayeds


def read_stata_delayed_group(
    file_names, meta=None, meta_chunksize=10000, *args, **kwargs
):

    # Create delayed objects for every file
    delayeds = [read_stata_delayed_chunked(f) for f in file_names]

    # Type metadata
    if meta is None:
        meta = get_stata_meta(file_names[0], *args, **kwargs)

    return dd.from_delayed(list(chain.from_iterable(delayeds)), meta=meta)


# Usage:
# files = ["a.dta","b.dta"]
# df = read_stata_delayed_group(files)
# agg = df.groupby(["exporter", "year"]).export_value.sum()
# agg.visualize()
# with ProgressBar():
#     result = agg.compute()
