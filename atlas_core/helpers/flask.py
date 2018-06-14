from flask import make_response, jsonify

from functools import wraps
from json import JSONEncoder


class APIError(Exception):
    """A Base API error class to be raised, that returns a json-api compliant
    error message."""

    TITLE = "An error occurred while processing your request."

    def __init__(self, status_code, message=None, payload=None, headers=None):
        Exception.__init__(self)
        self.status_code = status_code
        self.message = message
        self.payload = payload
        self.headers = headers

    def to_dict(self):
        rv = {}
        rv["payload"] = dict(self.payload or ())
        rv["status_code"] = self.status_code
        #rv["headers"] = self.headers
        rv["message"] = self.message
        return {"errors": rv}

    def __str__(self):
        return str(self.to_dict())


def handle_api_error(error):
    """Error handler for flask that handles :py:class:`~APIError` instances."""
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    if error.headers:
        for key, value in error.headers.items():
            response.headers[key] = value
    return response


def abort(status_code, message=None, payload=None, headers={}):
    raise APIError(status_code, message, payload, headers)


def headers(headers={}):
    """Decorator that adds custom HTTP headers to the response."""
    def decorator(f):
        @wraps(f)
        def inner(*args, **kwargs):
            response = make_response(f(*args, **kwargs))
            for header, value in headers.items():
                response.headers[header] = value
            return response
        return inner
    return decorator


def register_config_endpoint(app, entity_types, datasets, endpoints, url_pattern="/config"):
    """Register an endpoint to /config that shows the dataset / endpoint /
    entity configuration of the current app."""

    def config():
        return jsonify(endpoints=endpoints, datasets=datasets,
                       entity_types=entity_types)

    app.add_url_rule(
        url_pattern,
        endpoint="config",
        view_func=config
    )

    return app


class ForgivingJSONEncoder(JSONEncoder):
    """If object has a to_json property, use that. Otherwise try to do a
    regular json encode. If that fails, return the repr() of the object as a
    string."""

    def default(self, obj):
        if hasattr(obj, "to_json"):
            return obj.to_json()
        else:
            try:
                return super(ForgivingJSONEncoder, self).default(obj)
            except TypeError:
                return repr(obj)
