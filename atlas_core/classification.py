from .interfaces import IClassification
from .sqlalchemy import object_as_dict


class SQLAlchemyClassification(IClassification):

    def __init__(self, model):
        self.model = model

    def get_all(self, level=None):
        q = self.model.query

        if level is not None:
            q = q.filter_by(level=level)

        return [object_as_dict(x) for x in q.all()]

    def get_by_id(self, id):
        entry = self.model.query.get(id)

        if entry is None:
            return None

        return object_as_dict(entry)

    def get_level_by_id(self, id):
        data = self.model.query.get(id)

        if data is None:
            return None

        return data.level
