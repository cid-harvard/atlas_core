from flask import make_response, jsonify

from functools import wraps


class APIError(Exception):

    def __init__(self, status_code, message=None, payload=None, headers=None):
        Exception.__init__(self)
        self.status_code = status_code
        self.message = message
        self.payload = payload
        self.headers = headers

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


def handle_api_error(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    response.headers.update(error.headers)
    return response


def abort(status_code, body=None, headers={}):
    """Abort function that does content negotiation to respond with a JSON
    error body for ajax requests. From http://flask.pocoo.org/snippets/97/ -
    public domain, by Jökull Sólberg Auðunsson.
    """
    raise APIError(status_code, body, headers)


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
