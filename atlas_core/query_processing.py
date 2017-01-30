from .interfaces import IClassification

from flask import request

class SQLAlchemyClassification(IClassification):

    def __init__(model):
        self.model = model

    def get_level_from_id(self, id):

        data = self.model.query.get(id)

        if data is None:
            return None

        return data.level



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


def query_matches_slice_fields(query_fields, slice_fields):
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


def match_query_to_slices(query, data_slices, endpoint, result_level):
    """Let's say you have a query, and it has a few fields it's requesting.
    Which data slices have all the fields the query is asking for?"""

    # Filter data slices down to those that are specified in the endpoint
    slices = {k: v for k, v in data_slices.items() if k in endpoint["slices"]}

    # First filter by the query_entities that the query supplies (e.g.
    # product_id=23)
    query_fields = query["query_entities"]
    matching_slices = []
    for slice_name, slice_conf in slices.items():
        matched, unmatched_fields = query_matches_slice_fields(query_fields, slice_conf["fields"])
        if matched:
            matching_slices.append((slice_name, unmatched_fields))

    if len(matching_slices) == 0:
        abort(400, "No matching data slices. All slices:\n {}\
              Query:\n{}".format(endpoint["slices"], query))
    if len(matching_slices) == 1:
        return matching_slices[0]
    elif len(matching_slices) > 1:
        # TODO: Gross.
        # Get the slices that only have the result level that we specified
        matching_slices = [
            x for x in matching_slices
            if result_level in list(x[1].values())[0]["levels_available"][0]]

        if len(matching_slices) == 1:
            return matching_slices[0]
        else:
            abort(400, "Too many matching data slices. All slices:\n {}\
                  Matched slices: {}\n Query:\n{}".format(endpoint["slices"],
                                                          matching_slices,
                                                          query))


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

    # Did the user specify a level for the results?
    result_level = query["result"].get("level", None)
    if result_level is None:
        # TODO in the future: have some notion of default levels to pick a
        # slice, e.g. country is default, so choose country_product_year rather
        # than department_product_year. As opposed to default slices.
        if "default_slice" in endpoint:
            result_level = endpoint["default_slice"]
        else:
            abort(400, "No result level specified by user, and no default exists. Endpoint:\n {}\
                    Query:\n{}".format(endpoint, query))

    query["result"]["level"] = result_level

    # Try to infer which data slice to use by looking at arguments
    # e.g. For the product_exporters endpoint, if you ask for a return level of
    # departments, we use department_product_year, otherwise
    # country_product_year.
    matched_slice_name, unmatched_fields = match_query_to_slices(query, data_slices, endpoint, result_level)
    matched_slice = data_slices[matched_slice_name]

    query["slice"] = matched_slice_name

    if len(unmatched_fields) != 1:
        # TODO: Perhaps one day support this use case
        abort(400, "We should have only one unmatched field. Unmatched:\n {}\
              Matched slices: {}\n Query:\n{}".format(unmatched_fields,
                                                      matched_slice, query))

    result_field_name, result_field = list(unmatched_fields.items())[0]
    query["result"]["type"] = result_field["type"]

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
