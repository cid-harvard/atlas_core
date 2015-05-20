from flask.ext.testing import TestCase

from . import create_app
from .core import db
from atlas_core import factories


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


class TestCat(BaseTestCase):

    SQLALCHEMY_DATABASE_URI = "sqlite://:memory:"

    def test_get_cat(self):
        """Test to see if you can get a message by ID."""

        cat = factories.Cat()
        db.session.commit()

        response = self.client.get("/cats/" + str(cat.id))
        self.assert_200(response)
        resp_json = response.json
        self.assertEquals(resp_json["id"], str(cat.id))
        self.assertEquals(resp_json["born_at"], cat.born_at)
        self.assertEquals(resp_json["name"], cat.name)
