from flask import Flask

from werkzeug.contrib.profiler import ProfilerMiddleware

from .core import db


def load_config(app, additional_config={}):
    """Load configuration from environment variable plus from additional
    dictionary for test cases."""
    app.config.from_envvar("FLASK_CONFIG")
    app.config.update(additional_config)
    return app


def add_profiler(app):
    """Add a profiler that runs on every request when PROFILE set to True."""
    if app.config.get("PROFILE", False):
        app.wsgi_app = ProfilerMiddleware(app.wsgi_app,
                                          restrictions=[30],
                                          sort_by=("time", "cumulative"))
    return app


def create_db(app, db):
    """Create database from models."""
    with app.app_context():
        db.create_all()


def create_app(additional_config={}, name="atlas_core", standalone=False):
    """App factory. Creates a Flask `app` object and imports extensions, sets
    config variables etc."""

    app = Flask(name)
    app = load_config(app, additional_config)

    # Load extensions
    db.init_app(app)

    # Debug tools
    if app.debug:
        app = add_profiler(app)

    if standalone:
        create_db(app, db)

    return app
