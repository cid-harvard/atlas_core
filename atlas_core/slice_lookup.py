from .interfaces import ILookupStrategy


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
            value = query[key]

            # Generate predicate e.g. model.location_id==5
            predicate = (key_column == value)

            filter_predicates.append(predicate)

        return self.model.query.filter(filter_predicates)


class DataFrameLookup(ILookupStrategy):
    """Look up a query in a pandas dataframe."""

    def __init__(self, df, schema=None):
        self.df = df
        self.schema = schema

    def fetch(self, slice_def, query):
        raise NotImplementedError()


