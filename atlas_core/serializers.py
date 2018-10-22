from .interfaces import ISerializerStrategy

from flask import jsonify, current_app, request


def simplify_obj(obj):
    if hasattr(obj, "_asdict"):
        """SQLAlchemy result sets"""
        return obj._asdict()
    else:
        return repr(obj)


def ensure_simple(obj, simplify_func=simplify_obj):
    """Take an object and try to traverse it and ensuring all its
    sub-objects are simple to serialize primitive objects. If not, call repr()
    or some other function on them to convert them into a primitive. This is
    slow and is best-effort: It's for stuff like error messages, showing the
    user some unpredictable payload that may or may not contain random objects
    of any type. It isn't really meant to be used on large and predictable
    objects like API responses - in those cases you probably want to process
    those manually and systematically to make sure no non-serializable items
    are in there in the first place."""
    if type(obj) in (str, int, float, bool):
        return obj
    elif obj is None:
        return None
    elif type(obj) == list:
        return [ensure_simple(x) for x in obj]
    elif type(obj) == tuple:
        return tuple(ensure_simple(x) for x in obj)
    elif type(obj) == set:
        return set(ensure_simple(x) for x in obj)
    elif type(obj) == dict:
        return {ensure_simple(k): ensure_simple(v) for k, v in obj.items()}
    else:
        # If we don't know what to do with it
        return simplify_func(obj)


def get_serializer(serializer=None):

    # Parameter manually passed in has higher precedence than what the API
    # client specifies through query params, etc.
    if serializer is None:
        # If none manually passed in, infer from query parameters
        serializer = request.args.get("serializer", None)
        # Alternatively, we could use the `Accept` request header, but parsing
        # this is a bit more tricky.

    if serializer:
        if serializer in current_app.serializers:
            return current_app.serializers[serializer]
        else:
            from .helpers.flask import abort  # avoid circular import

            abort(400, message="Serializer {} does not exist".format(serializer))
    else:
        default_serializer = current_app.config.get("default_serializer", None)
        if default_serializer:
            return current_app.serializers[default_serializer]
        else:
            return JsonifySerializer


class JsonifySerializer(ISerializerStrategy):
    """Just uses flask.jsonify."""

    def serialize(self, *args, **kwargs):
        return jsonify(*args, **kwargs)


class MsgpackSerializer(ISerializerStrategy):
    """For custom default= function for serializing custom types, check out
    flask's jsonify implementation."""

    def __init__(self):
        import msgpack
        self.msgpack = msgpack

    def serialize(self, *args, **kwargs):
        if args and kwargs:
            raise TypeError("behavior undefined when passed both args and kwargs")
        elif len(args) == 1:  # single args are passed directly to dumps()
            data = args[0]
        else:
            data = args or kwargs

        return current_app.response_class(
            self.msgpack.packb(data) + b"\n", mimetype="application/x-msgpack"
        )


class UjsonSerializer(ISerializerStrategy):

    def __init__(self):
        import ujson
        self.ujson = ujson

    def serialize(self, *args, **kwargs):
        if args and kwargs:
            raise TypeError("behavior undefined when passed both args and kwargs")
        elif len(args) == 1:  # single args are passed directly to dumps()
            data = args[0]
        else:
            data = args or kwargs

        return current_app.response_class(
            self.ujson.dumps(
                data,
                ensure_ascii=current_app.config.get("JSON_AS_ASCII", True),
                sort_keys=current_app.config.get("JSON_SORT_KEYS", True),
            )
            + "\n",
            mimetype="application/json",
        )
