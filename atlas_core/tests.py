import json
import copy

from flask import request, jsonify
import pytest
import marshmallow as ma

from . import create_app
from .core import db
from .helpers.flask import APIError
from .sqlalchemy import BaseModel
from .model_mixins import IDMixin
from .testing import BaseTestCase

from .query_processing import *
from .slice_lookup import SQLAlchemyLookup


class ProductClassificationTest(object):
    def get_level_from_id(self, id):
        if id in [23, 30]:
            return "4digit"
        else:
            return None


class LocationClassificationTest(object):
    def get_level_from_id(self, id):
        if id in [23, 30]:
            return "department"
        else:
            return None


class SQLAlchemyLookupStrategyTest(object):
    def fetch(self, slice_def, query):
        return jsonify(data=[{"a": 1}, {"b": 2}, {"c": 3}])


entities = {
    "product": {
        "classification": ProductClassificationTest(),
    },
    "location": {
        "classification": LocationClassificationTest(),
    },
}

endpoints = {
    "product": {
        "url_pattern": "/data/product/",
        "arguments": [],
        "returns": ["product", "year"],  # ?level= is for the return variable of this
        "slices": ["product_year"],
    },
    "product_exporters": {
        "url_pattern": "/data/product/<int:product_id>/exporters/",
        "arguments": ["product"],
        "returns": ["location", "year"],
        "slices": ["country_product_year", "department_product_year"],
        "default_slice": "department_product_year",
    },
}


data_slices = {
    "product_year": {
        "fields": {
            "product": {
                "type": "product",
                "levels_available": ["section", "4digit"],  # subset of all available - based on data.
            },
        },
        "lookup_strategy": SQLAlchemyLookupStrategyTest(),
    },
    "country_product_year": {
        "fields": {
            "product": {
                "type": "product",
                "levels_available": ["section", "4digit"],  # subset of all available - based on data.
            },
            "location": {
                "type": "location",
                "levels_available": ["country"],
            },
        },
        "lookup_strategy": SQLAlchemyLookupStrategyTest(),
    },
    "department_product_year": {
        "fields": {
            "product": {
                "type": "product",
                "levels_available": ["section", "4digit"],  # subset of all available - based on data.
            },
            "location": {
                "type": "location",
                "levels_available": ["department"],
            },
        },
        "lookup_strategy": SQLAlchemyLookupStrategyTest(),
    },
}

# The URL as it comes in
query_url = "/data/product/23/exporters/?level=department"

# Just using the URL, we perform some inference on the query:
query_simple = {
    "endpoint": "product_exporters",  # Inferred from URL pattern
    "result": {
        "level": "department",  # Inferred from query param
    },
    "query_entities": [
        {
            "type": "product",  # Inferred from URL pattern
            "value": 23,  # Inferred from URL pattern
        },
    ]
}

# After a quick lookup on the argument ids in the metadata tables, we fill in
# missing levels
query_with_levels = {
    "endpoint": "product_exporters",
    "result": {
        "level": "department",
    },
    "query_entities": [
        {
            "type": "product",
            "level": "4digit",  # Inferred from the product id
            "value": 23,
        },
    ]
}

# Finally, with all we have, we can now look up slices that match our query, by
# trying to match the arguments / levels we want to the ones the slices have.
query_full = {
    "endpoint": "product_exporters",
    "slice": "department_product_year",  # can be inferred from the endpoint + arguments
    "result": {
        "type": "location",  # can be inferred from the selected slice or level??
        "level": "department",  # can be inferred from the selected slice default or taken from query param
    },
    "query_entities": [
        {
            "type": "product",
            "level": "4digit",
            "value": 23,
        },
    ]
}

class QueryBuilderTest(BaseTestCase):

    def setUp(self):
        self.app = create_app({
            #"SQLALCHEMY_DATABASE_URI": "sqlite://",
            "TESTING": True
        })
        self.test_client = self.app.test_client()

        @self.app.route("/data/product/")
        def product():
            return "hi"

        @self.app.route("/data/product/<int:product_id>/exporters/")
        def product_exporters(product_id):
            return "hello"

    def test_001_url_to_query(self):
        response = self.test_client.get("/data/product/23/exporters/")
        assert response.status_code == 200
        assert response.data == b"hello"

        with self.app.test_request_context("/data/product/23/exporters/?level=department"):
            assert request.path == "/data/product/23/exporters/"
            assert request.args["level"] == "department"

            assert query_simple == request_to_query(request)

        with self.app.test_request_context("/data/product/?level=4digit"):
            assert request.path == "/data/product/"
            assert request.args["level"] == "4digit"

            expected = {
                'endpoint': 'product',
                'query_entities': [
                ],
                'result': {'level': '4digit'}
            }
            assert expected == request_to_query(request)

    def test_002_infer_levels(self):
        with self.app.test_request_context("/data/product/23/exporters/?level=department"):

            # Test the happy path
            assert query_with_levels == infer_levels(query_simple, entities)

            # Change entity type to something bad
            query_bad_type = copy.deepcopy(query_simple)
            query_bad_type["query_entities"][0]["type"] = "non_existent"
            with pytest.raises(APIError) as exc:
                infer_levels(query_bad_type, entities)
            assert "Cannot find entity type" in str(exc.value)

            # Change entity id to something we know doesn't exist
            query_bad_id = copy.deepcopy(query_simple)
            query_bad_id["query_entities"][0]["value"] = 12345
            with pytest.raises(APIError) as exc:
                infer_levels(query_bad_id, entities)
            assert "Cannot find" in str(exc.value)
            assert "object with id 12345" in str(exc.value)


    def test_003_match_query(self):
        with self.app.test_request_context("/data/product/23/exporters/?level=department"):
            assert query_full == match_query(query_with_levels, data_slices, endpoints)

            # No result level, fill by default
            query_no_level = copy.deepcopy(query_with_levels)
            del query_no_level["result"]["level"]
            assert query_full == match_query(query_no_level, data_slices, endpoints)

            # Change endpoint to something we know doesn't exist
            query_bad_endpoint = copy.deepcopy(query_with_levels)
            query_bad_endpoint["endpoint"] = "potato"
            with pytest.raises(APIError) as exc:
                match_query(query_bad_endpoint, data_slices, endpoints)
            assert "is not a valid endpoint" in str(exc.value)

            # Change level to something else to make it not match
            query_bad_level = copy.deepcopy(query_with_levels)
            query_bad_level["query_entities"][0]["level"] = "test"
            with pytest.raises(APIError) as exc:
                match_query(query_bad_level, data_slices, endpoints)
            assert "no matching slices" in str(exc.value)

            # No result level, and no default specified
            query_no_default = copy.deepcopy(query_with_levels)
            endpoints_no_default = copy.deepcopy(endpoints)
            del endpoints_no_default["product_exporters"]["default_slice"]
            del query_no_default["result"]["level"]
            with pytest.raises(APIError) as exc:
                match_query(query_no_default, data_slices, endpoints_no_default)
            assert "No result level" in str(exc.value)

            # No matching slices
            endpoints_no_slices = copy.deepcopy(endpoints)
            endpoints_no_slices["product_exporters"]["slices"] = []
            with pytest.raises(APIError) as exc:
                match_query(query_with_levels, data_slices, endpoints_no_slices)
            assert "no matching slices" in str(exc.value)

            # No too many matching fields
            data_slices_modified = copy.deepcopy(data_slices)
            data_slices_modified["department_product_year"]["fields"]["otherfield"] = {
                "type": "occupation",
                "levels_available": ["2digit"],
            }
            with pytest.raises(APIError) as exc:
                match_query(query_with_levels, data_slices_modified, endpoints)
            assert "only one unmatched field" in str(exc.value)


        with self.app.test_request_context("/data/product/?level=4digit"):
            query = {
                'endpoint': 'product',
                'query_entities': [
                ],
                'result': {'level': '4digit'}
            }
            expected = {
                'endpoint': 'product',
                'slice': 'product_year',
                'query_entities': [
                ],
                'result': {'level': '4digit', 'type': 'product'}
            }
            assert expected == match_query(query, data_slices, endpoints)



    def test_004_query_result(self):
        with self.app.test_request_context("/data/product/23/exporters/?level=department"):
            assert request.path == "/data/product/23/exporters/"
            assert request.args["level"] == "department"

            # Request object comes in from the flask request object so we don't
            # have to pass it in
            api_response = flask_handle_query(entities, data_slices, endpoints)

            json_response = json.loads(api_response.get_data().decode("utf-8"))
            assert json_response["data"] == [{"a":1}, {"b":2}, {"c":3}]

        with self.app.test_request_context("/data/product/?level=4digit"):
            api_response = flask_handle_query(entities, data_slices, endpoints)

            json_response = json.loads(api_response.get_data().decode("utf-8"))
            assert json_response["data"] == [{"a": 1}, {"b": 2}, {"c": 3}]


class SQLAlchemySliceLookupTest(BaseTestCase):

    def setUp(self):
        super().__init__()

        class TestModel(BaseModel, IDMixin):
            __tablename__ = "test_model"
            product_id = db.Column(db.Integer)
            product_level = db.Column(db.Enum("section", "2digit", "4digit"))
            location_id = db.Column(db.String)
            location_level = db.Column(db.Enum("city", "department"))

            year = db.Column(db.Integer)
            export_value = db.Column(db.Integer)

        self.model = TestModel
        self.model.__table__.create(bind=db.engine)

        data = [
            [1, "section", 1, "department", 2007, 1000],
            [2, "section", 1, "department", 2007, 100],
            [1, "section", 1, "department", 2008, 1100],
            [2, "section", 1, "department", 2008, 110],
            [1, "section", 2, "department", 2007, 2000],
            [2, "section", 2, "department", 2007, 200],
            [1, "section", 2, "department", 2008, 2100],
            [2, "section", 2, "department", 2008, 210],
            [3, "4digit", 1, "department", 2007, 1000],
            [4, "4digit", 1, "department", 2007, 100],
            [3, "4digit", 1, "department", 2008, 1100],
            [4, "4digit", 1, "department", 2008, 110],
            [3, "4digit", 2, "department", 2007, 2000],
            [4, "4digit", 2, "department", 2007, 200],
            [3, "4digit", 2, "department", 2008, 2100],
            [4, "4digit", 2, "department", 2008, 210],
            [3, "4digit", 3, "city", 2007, 1000],
            [4, "4digit", 4, "city", 2007, 100],
            [3, "4digit", 3, "city", 2008, 1100],
            [4, "4digit", 4, "city", 2008, 110],
            [3, "4digit", 3, "city", 2007, 2000],
            [4, "4digit", 4, "city", 2007, 200],
            [3, "4digit", 3, "city", 2008, 2100],
            [4, "4digit", 4, "city", 2008, 210],
        ]
        keys = ["product_id", "product_level", "location_id", "location_level", "year", "export_value"]
        data = [dict(zip(keys, i)) for i in data]
        db.engine.execute(self.model.__table__.insert(), data)

        class TestSchema(ma.Schema):
            class Meta:
                fields = ("export_value", "product_id", "location_id", "year")
        self.schema = TestSchema(many=True)

        # self.app = register_endpoints(self.app, entities, data_slices, endpoints)
        # self.test_client = self.app.test_client()

        self.slice_def = {
            "fields": {
                "product": {
                    "type": "product",
                    "levels_available": ["section", "4digit"],
                },
            },
        }

    def test_lookup(self):
        query = {
            "endpoint": "product_exporters",
            "slice": "department_product_year",
            "result": {
                "type": "location",
                "level": "department",
            },
            "query_entities": [
                {
                    "type": "product",
                    "level": "section",
                    "value": 1,
                },
            ]
        }
        lookup = SQLAlchemyLookup(self.model, self.schema, json=False)
        result = lookup.fetch(self.slice_def, query)
        expected = [
            {'year': 2007, 'location_id': '1', 'product_id': 1, 'export_value': 1000},
            {'year': 2008, 'location_id': '1', 'product_id': 1, 'export_value': 1100},
            {'year': 2007, 'location_id': '2', 'product_id': 1, 'export_value': 2000},
            {'year': 2008, 'location_id': '2', 'product_id': 1, 'export_value': 2100},
        ]
        assert result == expected


        query = {
            'endpoint': 'product',
            'slice': 'product_year',
            'query_entities': [],
            'result': {'level': '4digit', 'type': 'product'}
        }
        lookup = SQLAlchemyLookup(self.model, self.schema, json=False)
        result = lookup.fetch(self.slice_def, query)
        expected = [
            {'year': 2007, 'location_id': '1', 'product_id': 1, 'export_value': 1000},
            {'year': 2008, 'location_id': '1', 'product_id': 1, 'export_value': 1100},
            {'year': 2007, 'location_id': '2', 'product_id': 1, 'export_value': 2000},
            {'year': 2008, 'location_id': '2', 'product_id': 1, 'export_value': 2100},
        ]
        # Should return results only filtered by result_level which is 4digit
        assert len(result) == 16


class RegisterAPIsTest(BaseTestCase):

    def setUp(self):
        self.app = create_app({
            #"SQLALCHEMY_DATABASE_URI": "sqlite://",
            "TESTING": True
        })
        self.app = register_endpoints(self.app, entities, data_slices, endpoints)
        self.test_client = self.app.test_client()

    def test_query_result(self):
        response = self.test_client.get("/data/product/23/exporters/?level=department")
        json_response = json.loads(response.get_data().decode("utf-8"))
        assert json_response["data"] == [{"a": 1}, {"b": 2}, {"c": 3}]
