def make_id_dictionary(items, id_field='id'):
    """Take a list of dicts (that contain an id field and a bunch of other
    stuff) and turn it into a dict of ids

        e.g. [{'id':3, 'value':7}, {'id':4, "value":1}] into:
        {3:{'value':7}, 4:{'value':1}}

    This is useful when I want to return a list of items as a javascript object
    instead of an array, to make life easier for frontend.

    :param id_field: Name of the dict key to use as id.
    """
    ret = {}
    for item in items:
        assert id_field in item, "Each element must have an id field"
        id_val = item.pop(id_field)
        ret[id_val] = item

    return ret
