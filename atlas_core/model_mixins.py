import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_method


class IDMixin(object):
    """Adds in an autoincremented integer ID primary key."""

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)

    def __repr__(self):
        return "<{0}: {1}>".format(self.__class__.__name__, self.id)


class I18nMixinBase(object):
    @hybrid_method
    def get_localized(self, field, lang):
        """Look up the language localized version of a field by looking up
        field_lang."""
        return getattr(self, field + "_" + lang)

    @staticmethod
    def create(fields, languages=["en"], class_name="I18nMixin"):
        localized_fields = {}
        for name, value in fields.items():
            for language in languages:
                field_name = name + "_" + language
                localized_fields[field_name] = sa.Column(value)
        return type(class_name, (I18nMixinBase,), localized_fields)
