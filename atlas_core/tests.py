from . import factories
from .core import db
from .testing import BaseTestCase


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
