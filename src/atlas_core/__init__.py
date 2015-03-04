from flask import Flask

from werkzeug.contrib.profiler import ProfilerMiddleware

from atlas_core.views import CatAPI
from atlas_core.views import api, cache
from atlas_core.models import db


def create_app(config={}):
    app = Flask("atlas_core")
    app.config.from_envvar("FLASK_CONFIG")
    app.config.update(config)

    #API Endpoints
    api.add_resource(CatAPI, "/cats/<int:cat_id>")

    #External
    api.init_app(app)
    cache.init_app(app)

    #Internal
    db.init_app(app)

    with app.app_context():
        db.create_all()

    #Debug tools
    if app.debug:
        if app.config.get("PROFILE", False):
            app.wsgi_app = ProfilerMiddleware(app.wsgi_app,
                                              restrictions=[30],
                                              sort_by=("time", "cumulative"))

    return app

