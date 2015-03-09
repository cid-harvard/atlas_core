from sqlalchemy.orm import Query, Model
from flask import abort


class BaseQuery(Query):

    def get_or_abort(self, obj_id, http_code=404):
        """Get an object or return an error code."""
        result = self.get(obj_id)
        return result or abort(http_code)

    def first_or_abort(self, obj_id, http_code=404):
        """Get first result or return an error code."""
        result = self.first()
        return result or abort(http_code)

    def filter_by_enum(self, enum, value, possible_values=None, http_code=400):
        """
        Filters a query object by an enum, testing that it got a valid value.

        :param enum: Enum column from model, e.g. Vehicle.type
        :param value: Value to filter by
        :param possible_values: None or list of acceptable values for `value`
        """
        if value is None:
            return self

        if possible_values is None:
            possible_values = enum.property.columns[0].type.enums

        if value not in possible_values:
            msg = "Expected one of: {0}, got {1}"\
                .format(possible_values, value)
            abort(http_code, message=msg)

        return self.filter(enum == value)


class BaseModel(Model):
    __abstract__ = True
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }
    query_class = BaseQuery
