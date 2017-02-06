from flask import request, jsonify

from .helpers.flask import abort

def request_to_query(request):

    query = {
        "endpoint": request.url_rule.endpoint,
        "result": {
            "level": request.args.get("level", None),
        }
    }

    query_entities = []
    for k, v in request.view_args.items():

        if k.endswith("_id"):
            k = k[:-3]

        entity = {
            "type": k,
            "value": v
        }
        query_entities.append(entity)

    query["query_entities"] = query_entities
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
    raise abort(400, body=msg)


def slice_fields_match_query(query_fields, slice_fields):
    """Does this slice have all the fields that the query needs? If so, it also
    returns the still-unmatched fields in the slice. Helper to
    match_query_to_slices that checks only one slice. """

    slice_fields = slice_fields.copy()

    for needed_field in query_fields:

        # Which fields in this slice have the same type/level as our field?
        matched = [
            field_name for field_name, field_conf
            in slice_fields.items()
            if needed_field["type"] == field_conf["type"] and
            needed_field["level"] in field_conf["levels_available"]
        ]

        if len(matched) >= 1:
            # Now that this field is matched, it can't be used again.
            del slice_fields[matched[0]]
        else:
            return (False, {})

    return (True, slice_fields)


def match_query_to_slices(query, endpoint_slices):
    """Let's say you have a query, and it has a few fields it's requesting.
    Which data slices have all the fields the query is asking for?"""

    # Filter by the query_entities that the query supplies (e.g. product_id=23)
    query_fields = query["query_entities"]
    matching_slices = []
    for slice_name, slice_conf in endpoint_slices.items():
        matched, unmatched_fields = slice_fields_match_query(query_fields, slice_conf["fields"])
        if matched:
            matching_slices.append((slice_name, unmatched_fields))

    return matching_slices


def infer_levels(query, entities):
    query = query.copy()

    # Fill in missing bits from query, by using the entity definitions
    # e.g. we can use the table of locations to verify location ids and fill in
    # their levels (dept, city, etc)
    for entity in query["query_entities"]:

        classification = entities[entity["type"]]["classification"]

        # Check values are valid for given types
        # (Is 23 really a valid location id?)
        # Infer level if missing
        # (Is id 23 a city or country?)
        if "level" not in entity:
            entry = classification.get_level_from_id(entity["value"])
            if entry is None:
                abort("Cannot find {} object with id {}. Query:\n\
                    {}".format(entity["type"], entity["value"], query))
            entity["level"] = entry

    return query


def match_query(query, data_slices, endpoints):
    query = query.copy()

    if query["endpoint"] not in endpoints:
        raise ValueError("{} is not a valid endpoint. Query:\n\
                         {}".format(query["endpoint"], query))

    endpoint = endpoints[query["endpoint"]]

    # Filter data slices down to those that are specified in the endpoint
    endpoint_slices = {k: v for k, v in data_slices.items() if k in endpoint["slices"]}

    # Try to infer which data slice to use by looking at arguments
    # e.g. For the product_exporters endpoint, if you ask for a return level of
    # departments, we use department_product_year, otherwise
    # country_product_year.
    matching_slices = match_query_to_slices(query, endpoint_slices)

    if len(matching_slices) == 0:
        abort(400, "There are no matching slices for your query. Endpoint:\n {}\
                Query:\n{}".format(endpoint, query))

    # Did the user specify a level for the results?
    result_level = query["result"].get("level", None)
    if result_level is None:

        # If no, is there at least a default slice specified?
        if "default_slice" not in endpoint:
            abort(400, "No result level specified by user, and no default exists. Endpoint:\n {}\
                    Query:\n{}".format(endpoint, query))

        # TODO in the future: have some notion of default levels to pick a
        # slice, e.g. country is default, so choose country_product_year rather
        # than department_product_year. As opposed to default slices.

        # Is the default slice actually valid (i.e. in the list of matched slices?)
        matching_slices = [(name, unmatched_fields) for name, unmatched_fields
                            in matching_slices
                            if endpoint["default_slice"] in name]

    else:
        # If there already is a result_level specified we can use that to
        # further filter the matches
        matching_slices = [(name, unmatched_fields) for name, unmatched_fields
                           in matching_slices
                           if result_level in list(unmatched_fields.values())[0]["levels_available"][0]]

    # After all this, we should have only one match remaining
    if len(matching_slices) != 1:
        abort(400, "Wrong number of matching data slices. All slices:\n {}\
                Matched slices: {}\n Query:\n{}".format(endpoint["slices"],
                                                        matching_slices,
                                                        query))

    matched_slice_name, unmatched_fields = matching_slices[0]
    matched_slice = data_slices[matched_slice_name]

    # There should also be one unmatched field in the slice (which is the
    # result field)
    if len(unmatched_fields) != 1:
        abort(400, "We should have only one unmatched field. Unmatched:\n {}\
              Matched slices: {}\n Query:\n{}".format(unmatched_fields,
                                                      matched_slice, query))

    result_field_name, result_field = list(unmatched_fields.items())[0]
    query["result"]["type"] = result_field["type"]

    if result_level is None and len(result_field["levels_available"]) > 1:
        abort(400, "It's unclear what result level we should be using! Result\
              field: {}\n Query:\n {}".format(result_field, query))
    elif result_level is None:
        result_level = result_field["levels_available"][0]

    query["result"]["level"] = result_level
    query["slice"] = matched_slice_name

    return query


def flask_handle_query(entities, data_slices, endpoints):
    """Function to use to bind to a flask route, that goes from a HTTP request
    to a query, to a response with data. """

    # Recover information from the HTTP request to create a detailed query
    # object
    query_simple = request_to_query(request)
    query_with_levels = infer_levels(query_simple, entities)
    query_full = match_query(query_with_levels, data_slices, endpoints)

    # Use query object to look up the data needed and return the response
    data_slice = data_slices[query_full["slice"]]
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
