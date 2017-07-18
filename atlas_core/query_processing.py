import copy

from flask import request

from .helpers.flask import abort


def request_to_query(request):

    query = {
        "endpoint": request.url_rule.endpoint,
        "result": {
            "level": request.args.get("level", None),
        }
    }

    arguments = {}
    for k, v in request.view_args.items():

        if k.endswith("_id"):
            k = k[:-3]

        arguments[k] = {
            "value": v,
        }

    query["arguments"] = arguments
    return query


def get_or_fail(name, dictionary):
    """Lookup a key in a dict, abort with helpful error on failure."""
    thing = dictionary.get(name, None)

    if thing is not None:
        return thing

    msg = "{} is not valid. Try one of: {}"\
        .format(
            name,
            list(dictionary.keys()))
    abort(400, message=msg)


def infer_levels(query, entities):
    query = copy.deepcopy(query)

    # Fill in missing bits from query, by using the entity definitions
    # e.g. we can use the table of locations to verify location ids and fill in
    # their levels (dept, city, etc)
    for arg_name, arg_query in query["arguments"].items():

        entity_config = entities.get(arg_query["type"], None)
        if entity_config is None:
            abort(400, "Cannot find entity type '{}'. Query:\n\
                  {}".format(arg_query["type"], arg_query["value"], query))

        classification = entity_config["classification"]

        # Check values are valid for given types
        # (Is 23 really a valid location id?)
        # Infer level if missing
        # (Is id 23 a city or country?)
        if "level" not in arg_query:
            entry = classification.get_level_by_id(arg_query["value"])
            if entry is None:
                abort(400, "Cannot find {} object with id {}. Query:\n\
                      {}".format(arg_query["type"], arg_query["value"], query))
            arg_query["level"] = entry

    return query


def match_query(query, datasets, endpoints):
    query = copy.deepcopy(query)

    dataset_conf = datasets[query["dataset"]]

    if query["result"]["level"] is None:
        abort(400, "You have not specified a result level(?level=foo).",
              payload=dict(query=query, dataset_conf=dataset_conf))

    slices = dataset_conf["slices"]
    filtered_slices = {}

    # Filter dataset's available slices on arguments and result levels
    for slice_name, slice_conf in slices.items():
        for arg_name, arg_query in query["arguments"].items():

            # If a level we're asking for isn't in the slice ...
            levels_available = slice_conf["levels"][arg_name]
            if arg_query["level"] not in levels_available:
                break  # Skip this slice.

        else:
            # If all argument levels were OK
            # And result level also is OK:
            result_name = query["result"]["name"]
            result_level = query["result"]["level"]
            levels_available = slice_conf["levels"][result_name]
            if(result_level in levels_available):
                filtered_slices[slice_name] = slice_conf

    if len(filtered_slices) == 0:
        abort(400, "There are no matching slices for your query in this dataset.",
              payload=dict(query=query, dataset_conf=dataset_conf))
    elif len(filtered_slices) == 2:
        abort(400, "There too many matching slices for your query in this dataset.",
              payload=dict(query=query, dataset_conf=dataset_conf))

    # We found the correct data slice!
    matched_slice_name = list(filtered_slices.keys())[0]
    query["slice"] = matched_slice_name

    # Fill in the argument / result field names
    facet_conf = dataset_conf["facets"]
    for arg_name, arg_query in query["arguments"].items():
        arg_query["field_name"] = facet_conf[arg_name]["field_name"]

    query["result"]["field_name"] = facet_conf[query["result"]["name"]]["field_name"]

    return query


def interpret_query(query, entities, datasets, endpoints):
    query = copy.deepcopy(query)

    if query["endpoint"] not in endpoints:
        abort(400, "{} is not a valid endpoint. Query:\n\
              {}".format(query["endpoint"], query))

    endpoint = endpoints[query["endpoint"]]
    dataset = datasets[endpoint["dataset"]]

    # Fill in the dataset name
    query["dataset"] = endpoint["dataset"]

    # Fill in argument and result types.
    for arg_name, arg_query in query["arguments"].items():
        arg_conf = dataset["facets"].get(arg_name, None)

        # Is this argument bogus?
        if arg_conf is None:
            abort(400, "{} is not a valid argument name. Query: {}".format(arg_name, query))

        # Fill in level
        arg_query["type"] = arg_conf["type"]

    # Get result from endpoint config
    result_name = endpoint["returns"][0]  # TODO: drop off year and assert len==1
    result_conf = dataset["facets"].get(result_name, None)

    if result_conf is None:
        abort(400, "{} is not a valid facet name.")

    # Fill in query result info
    query["result"]["type"] = result_conf["type"]
    query["result"]["name"] = result_name

    # TODO: Check result level against result type?
    return query


def flask_handle_query(entities, datasets, endpoints):
    """Function to use to bind to a flask route, that goes from a HTTP request
    to a query, to a response with data. """

    # Recover information from the HTTP request to create a detailed query
    # object
    query_simple = request_to_query(request)
    query_interpreted = interpret_query(query_simple, entities, datasets, endpoints)
    query_with_levels = infer_levels(query_interpreted, entities)
    query_full = match_query(query_with_levels, datasets, endpoints)

    # Use query object to look up the data needed and return the response
    dataset = datasets[query_full["dataset"]]
    data_slice = dataset["slices"][query_full["slice"]]
    lookup_strategy = data_slice["lookup_strategy"]
    return lookup_strategy.fetch(data_slice, query_full)


def register_endpoints(app, entities, data_slices, endpoints):

    def endpoint_handler_func(*args, **kwargs):
        return flask_handle_query(entities, data_slices, endpoints)

    for endpoint_name, endpoint_config in endpoints.items():
        app.add_url_rule(
            endpoint_config["url_pattern"],
            endpoint=endpoint_name,
            view_func=endpoint_handler_func
        )

    return app
