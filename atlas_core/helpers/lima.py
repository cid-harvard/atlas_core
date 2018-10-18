from .flask import abort
from ..interfaces import ISchemaStrategy


def marshal(schema, data):
    """Shortcut to marshal a lima schema and dump out a flask json response, or
    raise an APIError with appropriate messages otherwise."""

    try:
        serialization_result = schema.dump(data)
    except Exception as exc:
        raise abort(
            400, "Failed to serialize data", payload={"orig_exception": str(exc)}
        )

    return serialization_result


class LimaSchema(ISchemaStrategy):
    def __init__(self, schema):
        self.schema = schema

    def reshape(self, data):
        return marshal(self.schema, data)
