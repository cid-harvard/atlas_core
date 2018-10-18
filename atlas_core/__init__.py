from flask import Flask, cli

from werkzeug.contrib.profiler import ProfilerMiddleware

from .core import db
from .helpers.flask import APIError, handle_api_error
from .serializers import JsonifySerializer


def load_config(app, overrides={}):
    """Load configuration from environment variable plus from additional
    dictionary for test cases etc."""
    app.config.from_envvar("FLASK_CONFIG")
    app.config.update(overrides)
    return app


def add_profiler(app):
    """Add a profiler that runs on every request when PROFILE set to True."""
    if app.config.get("PROFILE", False):
        app.wsgi_app = ProfilerMiddleware(
            app.wsgi_app,
            restrictions=[30],
            sort_by=("time", "cumulative"),
            profile_dir=app.config.get("PROFILE_DIR", None),
        )
    return app


def create_db(app, db):
    """Create database from models."""
    with app.app_context():
        db.create_all()


def create_app(
    additional_config={},
    name="atlas_core",
    standalone=False,
    custom_json_encoder=False,
    load_dotenv=False,
):
    """App factory. Creates a Flask `app` object and imports extensions, sets
    config variables etc."""

    app = Flask(name)

    # Load environment variables from .env and .flaskenv including FLASK_APP
    # and FLASK_CONFIG etc etc. The flask 1.0+ CLI (`flask commandname`) does
    # this automatically if you have python-dotenv installed, but if you call
    # create_app() manually from your own code you need this. This needs to
    # happen before pretty much anything, so that we can customize even the
    # flask config location with this.
    if load_dotenv:
        cli.load_dotenv()

    # Load config from FLASK_CONFIG env variable.
    app = load_config(app, overrides=additional_config)

    # Load extensions
    db.init_app(app)

    # Debug tools
    if app.debug:
        app = add_profiler(app)

    if standalone:
        create_db(app, db)

    if app.config.get("CATCH_API_EXCEPTIONS", True):
        app.errorhandler(APIError)(handle_api_error)

    # For flask's jsonify
    if custom_json_encoder:
        app.json_encoder = custom_json_encoder

    # Register custom serializers like json, csv, msgpack, bson etc to use with
    # helpers.serialize()
    app.serializers = {"json": JsonifySerializer()}

    if "default_serializer" not in app.config:
        app.config["default_serializer"] = "json"

    return app
