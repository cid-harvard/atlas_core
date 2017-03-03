from .interfaces import ILookupStrategy
from .helpers import marshmallow
from .helpers import python as python_helpers

class SQLAlchemyLookup(ILookupStrategy):
    """Look up a query in an SQLAlchemy model."""

    def __init__(self, model, schema=None):
        self.model = model
        self.schema = schema

    def get_column_by_name(self, name):
        column = getattr(self.model, name, None)
        if column is None:
            raise ValueError("Column {} doesn't exist on model {}".format(name, self.model))
        return column

    def fetch(self, slice_def, query):
        # Build a lost of predicates
        # e.g. location_id==5 AND product_level=='4digit'

        filter_predicates = []
        for key, entity_def in slice_def["fields"].items():

            # Look up the details of the entity we're querying from query
            # object, e.g. location has id 5 and level blah
            query_entity = python_helpers.find_dict_in_list(
                query["query_entities"],
                type=key
            )

            if query_entity is None:
                raise ValueError("Can't find entity type {} in query: {}", key, query)

            # Get column name e.g. "location_id", and corresponding column
            # object, e.g. model.location_id
            entity_type = entity_def["type"]
            key_column = self.get_column_by_name(entity_type + "_id")

            # Generate predicate e.g. model.location_id==5
            predicate = (key_column == query_entity["value"])
            filter_predicates.append(predicate)

            # Do the same for the "level"
            level_column = self.get_column_by_name(entity_type + "_level")
            level_predicate = (level_column == query_entity["level"])
            filter_predicates.append(level_predicate)
            # TODO: how do we specify levels that don't need to be filtered by,
            # if the data already is partitioned? (e.g. geolevels)

        # Filter by result level also
        level_column = self.get_column_by_name(query["result"]["type"] + "_level")
        level_predicate = (level_column == query["result"]["level"])
        filter_predicates.append(level_predicate)

        q = self.model.query.filter(*filter_predicates)

        return marshmallow.marshal(self.schema, q, json=False)


class DataFrameLookup(ILookupStrategy):
    """Look up a query in a pandas dataframe."""

    def __init__(self, df, schema=None):
        self.df = df
        self.schema = schema

    def fetch(self, slice_def, query):
        raise NotImplementedError()


