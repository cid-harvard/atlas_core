from flask import Blueprint, jsonify
import marshmallow as m
from atlas_core.models import Cat

main_app = Blueprint("main_app", __name__)


class CatSchema(m.Schema):
    id = m.fields.Str()
    born_at = m.fields.Int()
    name = m.fields.Str()

cat_schema = CatSchema()


@main_app.route("/cats/<int:cat_id>")
def cat(cat_id):
    """Get a :py:class:`~atlas_core.models.Cat` with the given cat ID.

    :param id: unique ID of the cat
    :type id: int
    :code 404: cat doesn't exist

    """
    q = Cat.query.get_or_404(cat_id)
    return jsonify(cat_schema.dump(q).data)

