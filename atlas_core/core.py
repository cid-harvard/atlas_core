"""Contains common flask extensions."""

from flask.ext.sqlalchemy import SQLAlchemy
from flask.ext.babel import Babel

#: Flask-SQLAlchemy db object
db = SQLAlchemy()

#: Flask-Babel for internationalization
babel = Babel()
