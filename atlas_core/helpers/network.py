import pandas as pd
import json

from . import json_helpers as j


def read_network(file_name, nodes_field="nodes", edges_field="edges"):
    network = j.json_read(file_name)
    nodes = network[nodes_field]
    edges = network[edges_field]
    other_fields = {x: network[x] for x in network.keys()
                    if x not in [nodes_field, edges_field]}

    return pd.DataFrame.from_records(nodes), pd.DataFrame.from_records(edges), other_fields


def to_records(df):
    """Replacement for pandas' to_dict(orient="records") which has issues with
    upcasting ints to floats in the case of other floats being there.

    https://github.com/pandas-dev/pandas/issues/12859
    """
    return json.loads(df.to_json(orient="records"))


def write_network(file_name, nodes, edges, other_fields=None, nodes_field="nodes", edges_field="edges"):
    with open(file_name, "w") as f:
        network = {}
        network[nodes_field] = to_records(nodes)
        network[edges_field] = to_records(edges)
        if other_fields is not None:
            network.update(other_fields)
        f.write(json.dumps(network, indent=4, separators=(',', ': ')))


def remap_network_ids(nodes, edges, conversion_mapping, id_field="id", source_field="source", target_field="target"):
    """conversion_mapping is a series where the index is the old ids, and the
    values are the new ones."""

    conversion_mapping = conversion_mapping.copy()
    conversion_mapping.columns = [id_field]

    nodes = nodes.merge(conversion_mapping,
                        left_on=id_field, right_index=True,
                        how="left", suffixes=("_old", ""))

    nodes = nodes.drop([x for x in nodes.columns if x.endswith("_old")], axis=1)

    # merge-and-replace
    # Merge column renaming semantics

    conversion_mapping.columns = [source_field]
    edges = edges\
        .merge(conversion_mapping,
               left_on=source_field, right_index=True,
               how="left", suffixes=("_old", ""))

    conversion_mapping.columns = [target_field]
    edges = edges\
        .merge(conversion_mapping,
               left_on=target_field, right_index=True,
               how="left", suffixes=("_old", ""))

    edges = edges.drop([x for x in edges.columns if x.endswith("_old")], axis=1)

    return nodes, edges


def find_neighbors(nodes, edges, this_node, extra_fields=[]):
    connected_edges = edges[(edges.source == this_node) | (edges.target == this_node)].copy()

    def get_other_node(this_node):
        def inner(df):
            if df.source == this_node:
                return int(df.target)
            else:
                return int(df.source)
        return inner

    connected_edges["id"] = connected_edges.apply(get_other_node(this_node), axis=1)
    connected_edges = connected_edges[["id"] + extra_fields]

    return to_records(connected_edges)
