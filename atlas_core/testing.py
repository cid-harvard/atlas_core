from flask.ext.testing import TestCase

from . import create_app
from .core import db


class BaseTestCase(TestCase):
    """Base TestCase to add in convenience functions, defaults and custom
    asserts. Uses app factory and creates / tears down db.
    """

    SQLALCHEMY_DATABASE_URI = "sqlite://"
    ADDITIONAL_CONFIG = {
        "SQLALCHEMY_DATABASE_URI": SQLALCHEMY_DATABASE_URI,
        "TESTING": True
    }

    def create_app(self):
        return create_app(additional_config=self.ADDITIONAL_CONFIG)

    def setUp(self):
        db.create_all()

    def tearDown(self):
        db.session.remove()
        db.drop_all()
