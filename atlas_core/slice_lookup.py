from .interfaces import ILookupStrategy
from .helpers import marshmallow
from .helpers import python as python_helpers

class SQLAlchemyLookup(ILookupStrategy):
    """Look up a query in an SQLAlchemy model."""

    def __init__(self, model, schema=None):
        self.model = model
        self.schema = schema

    def fetch(self, slice_def, query):

        # Build a lost of predicates
        # e.g. location_id==5 AND product_level=='4digit'

        filter_predicates = []
        for key, entity_def in slice_def["fields"].items():

            # Get column name e.g. "location_id"
            entity_type = entity_def["type"]
            key_column_name = entity_type + "_id"

            # Get column object from column name e.g. model.location_id
            key_column = getattr(self.model, key_column_name, None)
            if key_column is None:
                # raise
                pass

            # Look up value from query, e.g. location id 5
            query_entity = python_helpers.find_dict_in_list(
                query["query_entities"],
                type=key
            )

            # Generate predicate e.g. model.location_id==5
            if query_entity is not None:
                predicate = (key_column == query_entity["value"])
                filter_predicates.append(predicate)

        # Filter by result level also
        predicate = (self.model.level == query["result"]["level"])
        filter_predicates.append(predicate)

        query = self.model.query.filter(*filter_predicates)

        return marshmallow.marshal(self.schema, query)


class DataFrameLookup(ILookupStrategy):
    """Look up a query in a pandas dataframe."""

    def __init__(self, df, schema=None):
        self.df = df
        self.schema = schema

    def fetch(self, slice_def, query):
        raise NotImplementedError()


