from abc import ABC, abstractmethod


class IClassification(ABC):

    @abstractmethod
    def get_by_id(self, id):
        raise NotImplementedError()

    @abstractmethod
    def get_all(self, id):
        raise NotImplementedError()

    @abstractmethod
    def get_level_by_id(self, id):
        """Given an id, find the level. E.g. is id 23 a department or a
        city?"""
        raise NotImplementedError()

    """
    def get_classification(self, level=none):
        q = self.model.query

        if level is not none:
            level = (self.model.level == level)
            q = q.filter_by(level=level).all()

        return q.all()

    def get_level_mapping(self, from_level, to_level):
        pass

    """


class ILookupStrategy(ABC):
    @abstractmethod
    def fetch(self, slice_def, query):
        pass

