import unittest

from flask import request

from . import create_app
from .core import db
from .testing import BaseTestCase

from .query_processing import *
from .slice_lookup import SQLAlchemyLookup


class ProductClassificationTest(object):
    def get_level_from_id(self, id):
        return "4digit"


class LocationClassificationTest(object):
    def get_level_from_id(self, id):
        return "department"


class SQLAlchemyLookupStrategyTest(object):
    def fetch(self, slice_def, query):
        return [{"a":1}, {"b":2}, {"c":3}]


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
    "endpoint": "product_exporters", # Inferred from URL pattern
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

    def test_002_infer_levels(self):
        with self.app.test_request_context("/data/product/23/exporters/?level=department"):
            assert query_with_levels == infer_levels(query_simple, entities)

    def test_003_match_query(self):
        with self.app.test_request_context("/data/product/23/exporters/?level=department"):
            assert query_full == match_query(query_with_levels, data_slices, endpoints)

    def test_query_result(self):
        with self.app.test_request_context("/data/product/23/exporters/?level=department"):
            assert request.path == "/data/product/23/exporters/"
            assert request.args["level"] == "department"

            # Request object comes in from the flask request object so we don't
            # have to pass it in
            api_response = flask_handle_query(entities, data_slices, endpoints)

            assert api_response == [{"a":1}, {"b":2}, {"c":3}]
