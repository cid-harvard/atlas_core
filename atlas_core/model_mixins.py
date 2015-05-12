import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_method


class IDMixin(object):
    """Adds in an autoincremented integer ID primary key."""
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)

    def __repr__(self):
        return "<{0}: {1}>".format(self.__class__.__name__, self.id)


class LanguageMixin(object):
    """
    Mixin to include language support in a database object, plus convenience
    functions.
    """
    # TODO: Write a make_languages(lang_list, string_length) to have this not
    # be hardcoded values.

    #: English Language
    en = sa.Column(sa.String(50))

    #: Spanish Language
    es = sa.Column(sa.String(50))

    @hybrid_method
    def localized_name(self, lang):
        """Gets localized name of object. Hybrid method that works both in a db
        query and in python.

        :param lang:  Two-character primary language subtags of the language,
        according to IETF RFC 5646 section 2.2.1. E.g. 'fr' or 'en'."""
        return getattr(self, lang)
