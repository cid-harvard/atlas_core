import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_method


class IDMixin:
    """Adds in an autoincremented integer ID."""
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)

    def __repr__(self):
        return "<{0}: {1}>".format(self.__class__.__name__, self.id)


class LanguageMixin:
    """"
    Mixin to include language support in a database object, plus convenience
    functions.

    - TODO: Write a make_languages(lang_list, string_length) to have this not
    be hardcoded values.
    """
    en = sa.Column(sa.String(50))
    es = sa.Column(sa.String(50))

    @hybrid_method
    def localized_name(self, lang):
        return getattr(self, lang)
