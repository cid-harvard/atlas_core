from .interfaces import IClassification

class SQLAlchemyClassification(IClassification):

    def __init__(model):
        self.model = model

    def get_level_from_id(self, id):

        data = self.model.query.get(id)

        if data is None:
            return None

        return data.level
