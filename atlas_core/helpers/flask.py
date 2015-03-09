from werkzeug.exceptions import default_exceptions, HTTPException
from flask import make_response, abort as flask_abort, request
from flask.exceptions import JSONHTTPException

from functools import wraps


def abort(status_code, body=None, headers={}):
    """Abort function that does content negotiation to respond with a JSON
    error body for ajax requests. From http://flask.pocoo.org/snippets/97/ -
    public domain, by Jökull Sólberg Auðunsson.
    """

    if 'text/html' in request.headers.get("Accept", ""):
        error_cls = HTTPException
    else:
        error_cls = JSONHTTPException

    class_name = error_cls.__name__
    bases = [error_cls]
    attributes = {'code': status_code}

    if status_code in default_exceptions:
        # Mixin the Werkzeug exception
        bases.insert(0, default_exceptions[status_code])

    error_cls = type(class_name, tuple(bases), attributes)
    flask_abort(make_response(error_cls(body), status_code, headers))


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
