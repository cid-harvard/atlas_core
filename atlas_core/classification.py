from .core import db
from .interfaces import IClassification
from .sqlalchemy import object_as_dict

from sqlalchemy.orm import aliased

from functools import lru_cache


class SQLAlchemyClassification(IClassification):

    def __init__(self, model, levels):
        self.model = model
        self.levels = levels

    @lru_cache(maxsize=None)
    def get_all(self, level=None):
        q = self.model.query

        if level is not None:
            q = q.filter_by(level=level)

        return [object_as_dict(x) for x in q.all()]

    @lru_cache(maxsize=None)
    def get_by_id(self, id):
        entry = self.model.query.get(id)

        if entry is None:
            return None

        return object_as_dict(entry)

    @lru_cache(maxsize=None)
    def get_level_by_id(self, id):
        data = self.model.query.get(id)

        if data is None:
            return None

        return data.level

    @lru_cache(maxsize=None)
    def aggregation_mapping(self, from_level, to_level, names=False):
        """Return mapping from higher level x to lower level y"""

        assert from_level != to_level

        from_index = self.levels.index(from_level)
        to_index = self.levels.index(to_level)

        if not (from_index > to_index):
            raise ValueError("""{} is higher level than {}. Did you specify them
                             backwards?""".format(from_level, to_level))

        # Since we're going to have to create a series of self-joins to
        # traverse up the tree, generate as many aliases of the table as we
        # need so we can later refer to each instance of the table that
        # represents each intermediate level of the classification
        variables = [self.model]
        for _ in range(from_index - to_index):
            variables.append(aliased(self.model))

        # We're looking for a mapping from the ids of `from_level` to the ids
        # of `to_level`
        q = db.session.query(variables[0].id, variables[-1].id)\

        # Any row that isn't in from_level can be dropped
        q = q.filter(variables[0].level == from_level)

        # Join our parent_id to the id of the next level of the classification
        for i in range(from_index - to_index):
            q = q.join(
                variables[i + 1],
                variables[i + 1].id == variables[i].parent_id
            )

        return dict(q.all())
