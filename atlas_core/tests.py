import json
import copy

from flask import request, jsonify
import pytest
import marshmallow as ma

from . import create_app
from .core import db
from .classification import SQLAlchemyClassification
from .helpers.flask import APIError
from .sqlalchemy import BaseModel
from .model_mixins import IDMixin
from .testing import BaseTestCase

from .query_processing import (request_to_query, infer_levels,
                               interpret_query, match_query,
                               flask_handle_query, register_endpoints)
from .slice_lookup import SQLAlchemyLookup


class ProductClassificationTest(object):
    def get_level_by_id(self, id):
        if id in [23, 30]:
            return "4digit"
        else:
            return None


class LocationClassificationTest(object):
    def get_level_by_id(self, id):
        if id in [23, 30]:
            return "department"
        else:
            return None


class SQLAlchemyLookupStrategyTest(object):
    def fetch(self, slice_def, query, json=True):
        if json:
            return jsonify(data=[{"a": 1}, {"b": 2}, {"c": 3}])
        else:
            return [{"a": 1}, {"b": 2}, {"c": 3}]


entities = {
    "hs_product": {
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
        "returns": ["product", "year"],
        "dataset": "product_year",
    },
    "product_exporters": {
        "url_pattern": "/data/product/<int:product>/exporters/",
        "arguments": ["product"],
        "returns": ["location", "year"],
        "dataset": "location_product_year",
    },
}


datasets = {
    "product_year": {
        "facets": {
            "product": {
                "type": "hs_product",
                "field_name": "product_id"
            },
            "year": {
                "type": "year",
                "field_name": "year",
            },
        },
        "slices": {
            "product_year": {
                "levels": {
                    "product": ["section", "4digit"],
                },
                "lookup_strategy": SQLAlchemyLookupStrategyTest(),
            },
        }
    },
    "location_product_year": {
        "facets": {
            "product": {
                "type": "hs_product",
                "field_name": "product_id"
            },
            "location": {
                "type": "location",
                "field_name": "location_id"
            },
            "year": {
                "type": "year",
                "field_name": "year",
            },
        },
        "slices": {
            "country_product_year": {
                "levels": {
                    "location": ["country"],
                    "product": ["section", "4digit"],
                },
                "lookup_strategy": SQLAlchemyLookupStrategyTest(),
            },
            "department_product_year": {
                "levels": {
                    "location": ["department"],
                    "product": ["section", "4digit"],
                },
                "lookup_strategy": SQLAlchemyLookupStrategyTest(),
            }
        }
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
    "arguments": {
        "product": {
            "value": 23,  # Inferred from URL pattern
        },
    },
    'year_range': {
        'start': None,
        'end': None,
    },
}

# Then using the endpoint and dataset configs, we can decode more
query_interpreted = {
    "endpoint": "product_exporters",
    "dataset": "location_product_year",  # Inferred from endpoint config
    "result": {
        "name": "location",  # Inferred from dataset def
        "type": "location",  # Inferred from dataset def
        "level": "department",
    },
    "arguments": {
        "product": {
            "type": "hs_product",  # Inferred from arguments and dataset definition.
            "value": 23,
        },
    },
    'year_range': {
        'start': None,
        'end': None,
    },
}

# Consulting the classifications, we can decode the input levels
query_with_levels = {
    "endpoint": "product_exporters",
    "dataset": "location_product_year",
    "result": {
        "name": "location",
        "type": "location",
        "level": "department",
    },
    "arguments": {
        "product": {
            "type": "hs_product",
            "level": "4digit",  # Inferred from product id.
            "value": 23,
        },
    },
    'year_range': {
        'start': None,
        'end': None,
    },
}

# Finally, with all we have, we can now look up slices that match our query, by
# trying to match the arguments / levels we want to the ones the slices have.
query_full = {
    "endpoint": "product_exporters",
    "dataset": "location_product_year",
    "slice": "department_product_year",  # Inferred from argument and result levels
    "result": {
        "name": "location",
        "field_name": "location_id",  # Inferred from dataset / slice config
        "type": "location",
        "level": "department",
    },
    "arguments": {
        "product": {
            "field_name": "product_id",  # Inferred from dataset / slice config
            "type": "hs_product",
            "level": "4digit",
            "value": 23,
        },
    },
    'year_range': {
        'start': None,
        'end': None,
    },
}


class QueryBuilderTest(BaseTestCase):

    def setUp(self):
        self.app = create_app({
            # "SQLALCHEMY_DATABASE_URI": "sqlite://",
            "TESTING": True
        })
        self.test_client = self.app.test_client()

        @self.app.route("/data/product/")
        def product():
            return "hi"

        @self.app.route("/data/product/<int:product>/exporters/")
        def product_exporters(product):
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
                'arguments': {},
                'result': {'level': '4digit'},
                'year_range': {'start': None, 'end': None}
            }
            assert expected == request_to_query(request)

    def test_002_interpret_query(self):
        with self.app.test_request_context("/data/product/23/exporters/?level=department"):

            # Test the happy path
            assert query_interpreted == interpret_query(query_simple, entities, datasets, endpoints)

            # Change endpoint to something we know doesn't exist
            query_bad_endpoint = copy.deepcopy(query_simple)
            query_bad_endpoint["endpoint"] = "potato"
            with pytest.raises(APIError) as exc:
                interpret_query(query_bad_endpoint, entities, datasets, endpoints)
            assert "is not a valid endpoint" in str(exc.value)

            # TODO: these internal consistency checks could be done separately
            # in advance, during load time.
            # Check nonexistent dataset
            # Check nonexistent facet names

    def test_003_infer_levels(self):
        with self.app.test_request_context("/data/product/23/exporters/?level=department"):

            # Test the happy path
            assert query_with_levels == infer_levels(query_interpreted, entities)

            # Change entity type to something bad
            query_bad_type = copy.deepcopy(query_interpreted)
            query_bad_type["arguments"]["product"]["type"] = "non_existent"
            with pytest.raises(APIError) as exc:
                infer_levels(query_bad_type, entities)
            assert "Cannot find entity type" in str(exc.value)

            # Change entity id to something we know doesn't exist
            query_bad_id = copy.deepcopy(query_interpreted)
            query_bad_id["arguments"]["product"]["value"] = 12345
            with pytest.raises(APIError) as exc:
                infer_levels(query_bad_id, entities)
            assert "Cannot find" in str(exc.value)
            assert "object with id 12345" in str(exc.value)

            # TODO: Check against bogus result level

    def test_004_match_query(self):
        with self.app.test_request_context("/data/product/23/exporters/?level=department"):
            assert query_full == match_query(query_with_levels, datasets, endpoints)

            # Change level to something else to make it not match
            query_bad_level = copy.deepcopy(query_with_levels)
            query_bad_level["arguments"]["product"]["level"] = "test"
            with pytest.raises(APIError) as exc:
                match_query(query_bad_level, datasets, endpoints)
            assert "no matching slices" in str(exc.value)

            # Return level not specified
            query_bad_return_level = copy.deepcopy(query_with_levels)
            query_bad_return_level["result"]["level"] = None
            with pytest.raises(APIError) as exc:
                match_query(query_bad_return_level, datasets, endpoints)
            assert "result level" in str(exc.value)

            # No matching slices
            datasets_no_slices = copy.deepcopy(datasets)
            datasets_no_slices["location_product_year"]["slices"] = {}
            with pytest.raises(APIError) as exc:
                match_query(query_with_levels, datasets_no_slices, endpoints)
            assert "no matching slices" in str(exc.value)

            # Too many matching slices
            datasets_modified = copy.deepcopy(datasets)
            datasets_modified["location_product_year"]["slices"]["country_product_year"]["levels"]["location"] = ["country", "department"]
            with pytest.raises(APIError) as exc:
                match_query(query_with_levels, datasets_modified, endpoints)
            assert "too many matching slices" in str(exc.value)

        with self.app.test_request_context("/data/product/?level=4digit"):
            query = {
                'endpoint': 'product',
                'dataset': 'product_year',
                'arguments': {
                },
                'result': {'name': 'product', 'type': 'hs_product', 'level': '4digit'}
            }
            expected = {
                'endpoint': 'product',
                'dataset': 'product_year',
                'slice': 'product_year',
                'arguments': {
                },
                'result': {'name': 'product', 'type': 'hs_product', 'level': '4digit', 'field_name': 'product_id'}
            }
            assert expected == match_query(query, datasets, endpoints)

    def test_005_query_result(self):
        with self.app.test_request_context("/data/product/23/exporters/?level=department"):
            assert request.path == "/data/product/23/exporters/"
            assert request.args["level"] == "department"

            # Request object comes in from the flask request object so we don't
            # have to pass it in
            api_response = flask_handle_query(entities, datasets, endpoints)

            assert api_response == [{"a":1}, {"b":2}, {"c":3}]

        with self.app.test_request_context("/data/product/?level=4digit"):
            api_response = flask_handle_query(entities, datasets, endpoints)

            assert api_response == [{"a": 1}, {"b": 2}, {"c": 3}]


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
            "dataset": "location_product_year",
            "slice": "department_product_year",
            "result": {
                "name": "location",
                "field_name": "location_id",
                "type": "location",
                "level": "department",
            },
            "arguments": {
                "product": {
                    "field_name": "product_id",
                    "type": "hs_product",
                    "level": "section",
                    "value": 1,
                },
            },
            'year_range': {
                'start': None,
                'end': None,
            },
        }
        lookup = SQLAlchemyLookup(self.model, self.schema)

        result = lookup.fetch(self.slice_def, query, json=False)
        expected = [
            {'year': 2007, 'location_id': '1', 'product_id': 1, 'export_value': 1000},
            {'year': 2008, 'location_id': '1', 'product_id': 1, 'export_value': 1100},
            {'year': 2007, 'location_id': '2', 'product_id': 1, 'export_value': 2000},
            {'year': 2008, 'location_id': '2', 'product_id': 1, 'export_value': 2100},
        ]
        assert result.data == expected

        query["arguments"]["product"]["value"] = 2
        result = lookup.fetch(self.slice_def, query, json=False)
        expected = [
            {'year': 2007, 'location_id': '1', 'product_id': 2, 'export_value': 100},
            {'year': 2008, 'location_id': '1', 'product_id': 2, 'export_value': 110},
            {'year': 2007, 'location_id': '2', 'product_id': 2, 'export_value': 200},
            {'year': 2008, 'location_id': '2', 'product_id': 2, 'export_value': 210},
        ]
        assert result.data == expected


        query["arguments"]["product"]["value"] = 9999
        result = lookup.fetch(self.slice_def, query, json=False)
        expected = []
        assert result.data == expected

        query = {
            'endpoint': 'product',
            'slice': 'product_year',
            'arguments': {},
            'result': {'level': '4digit', 'type': 'product', 'field_name': 'product_id'},
            'year_range': {
                'start': None,
                'end': None,
            },
        }
        lookup = SQLAlchemyLookup(self.model, self.schema)
        # Should return results only filtered by result_level which is 4digit
        result = lookup.fetch(self.slice_def, query, json=False)
        assert len(result.data) == 16


class RegisterAPIsTest(BaseTestCase):

    def setUp(self):
        self.app = create_app({
            # "SQLALCHEMY_DATABASE_URI": "sqlite://",
            "TESTING": True
        })
        self.app = register_endpoints(self.app, entities, datasets, endpoints)
        self.test_client = self.app.test_client()

    def test_query_result(self):
        response = self.test_client.get("/data/product/23/exporters/?level=department")
        json_response = json.loads(response.get_data().decode("utf-8"))
        assert json_response["data"] == [{"a": 1}, {"b": 2}, {"c": 3}]


class SQLAlchemyClassificationTest(BaseTestCase):

    def setUp(self):
        self.app = create_app({
            "SQLALCHEMY_DATABASE_URI": "sqlite://",
            "TESTING": True
        })

        class TestClassification(BaseModel):
            __tablename__ = "test_classification"

            id = db.Column(db.Integer, primary_key=True)
            code = db.Column(db.Unicode(25))
            level = db.Column(db.Enum("top", "mid", "low", "bottom"))
            name = db.Column(db.String)
            parent_id = db.Column(db.Integer)

        self.model = TestClassification
        self.classification = SQLAlchemyClassification(self.model, ["top", "mid", "low", "bottom"])
        self.model.__table__.create(bind=db.engine)

        data = [
            [0, "A", "top", "Vehicles", None],
            [1, "A1", "mid", "Water Vehicles", 0],
            [2, "A10", "low", "Jet-ski", 1],
            [3, "A11", "low", "Boat", 1],
            [4, "A111", "bottom", "Rowboat", 3],
            [5, "A2", "mid", "Land Vehicles", 0],
            [6, "A20", "low", "Trucks", 5],
            [7, "A201", "bottom", "Regular Trucks", 6],
            [8, "A202", "bottom", "Dump Trucks", 6],
        ]
        keys = ["id", "code", "level", "name", "parent_id"]
        self.data = [dict(zip(keys, i)) for i in data]
        db.engine.execute(self.model.__table__.insert(), self.data)

    def test_all(self):
        assert self.classification.get_by_id(8) == {
            "id": 8,
            "code": "A202",
            "level": "bottom",
            "name": "Dump Trucks",
            "parent_id": 6
        }

        assert self.classification.get_level_by_id(2) == "low"
        assert self.classification.get_level_by_id(0) == "top"
        assert self.classification.get_level_by_id(8) == "bottom"

        assert self.classification.get_level_by_id(67) is None
        assert self.classification.get_by_id(66) is None

        assert self.classification.get_all() == self.data

        assert self.classification.get_all(level="bottom") == [x for x in self.data if x["level"] == "bottom"]

        assert self.classification.aggregation_mapping("bottom", "top") == {4: 0, 7: 0, 8: 0}
        assert self.classification.aggregation_mapping("bottom", "mid") == {4: 1, 7: 5, 8: 5}
        assert self.classification.aggregation_mapping("bottom", "low") == {4: 3, 7: 6, 8: 6}

        assert self.classification.aggregation_mapping("low", "top") == {2: 0, 3: 0, 6: 0}
        assert self.classification.aggregation_mapping("low", "mid") == {2: 1, 3: 1, 6: 5}

        assert self.classification.aggregation_mapping("mid", "top") == {1: 0, 5: 0}

        with pytest.raises(AssertionError):
            self.classification.aggregation_mapping("mid", "mid")

        with pytest.raises(ValueError):
            self.classification.aggregation_mapping("mid", "blah")

        with pytest.raises(ValueError):
            self.classification.aggregation_mapping("top", "bottom")
