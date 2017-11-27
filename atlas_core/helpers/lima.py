from flask import jsonify

from .flask import abort


def marshal(schema, data, json=True, many=None):
    """Shortcut to marshal a lima schema and dump out a flask json response, or
    raise an APIError with appropriate messages otherwise."""

    try:
        serialization_result = schema.dump(data)
    except Exception as exc:
        raise abort(400, "Failed to serialize data", payload={"orig_exception": str(exc)})

    if json:
        return jsonify(data=serialization_result)
    else:
        return serialization_result
