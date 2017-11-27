import marshmallow as ma
from flask import jsonify

from .flask import abort


def marshal(schema, data, json=True, many=True):
    """Shortcut to marshals a marshmallow schema and dump out a flask json
    response, or raise an APIError with appropriate messages otherwise."""

    try:
        serialization_result = schema.dump(data, many=many)
    except ma.ValidationError as err:
        raise abort(err.messages)

    if json:
        return jsonify(data=serialization_result.data)
    else:
        return serialization_result.data
