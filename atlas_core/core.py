"""Contains common flask extensions."""

from flask_sqlalchemy import SQLAlchemy

#: Flask-SQLAlchemy db object
db = SQLAlchemy()

# Need these to be generated in order to drop them when loading
db.metadata.naming_convention = {
    "ix": "%(table_name)s_%(column_0_name)s_idx",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "pk": "%(table_name)s_pkey",
}
