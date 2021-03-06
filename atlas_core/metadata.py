from flask import request

from .helpers.flask import abort
from .serializers import get_serializer


def make_metadata_api(classification, metadata_schema, api_metadata={}):
    """Since all metadata APIs look very similar, this function just generates
    the function that'll handle the API endpoint for an entity. It generates a
    function that handles both /metadata/entity/ and /metadata/entity/<id>."""

    def metadata_api(entity_id):
        """Get all :py:class:`~colombia.models.Metadata` s or a single one with the
        given id.

        :param id: Entity id, see :py:class:`colombia.models.Metadata.id`
        :type id: int
        :code 404: Entity doesn't exist
        """
        if entity_id is not None:
            q = classification.get_by_id(entity_id)
            data = metadata_schema.reshape([q])[0]
        else:
            level = request.args.get("level", None)
            q = classification.get_all(level=level)
            data = metadata_schema.reshape(q)

        return get_serializer().serialize(data=data, api_metadata=api_metadata)

    def hierarchy_api():
        """Get the mapping of ids from a level of a classification to another
        level.

        :param from_level: Entity level, see :py:class:`colombia.models.Metadata.level`
        :param to_level: Entity level, see :py:class:`colombia.models.Metadata.level`
        :type from_level: str
        :type to_level: str
        """
        from_level = request.args.get("from_level", classification.levels[-1])
        to_level = request.args.get("to_level", classification.levels[0])

        try:
            mapping = classification.aggregation_mapping(from_level, to_level)
        except (AssertionError, ValueError):
            abort(
                400,
                """Levels you gave me seem invalid. Are you sure
                  from_level is lower than to_level and either are valid
                  levels?""",
                payload=dict(levels=classification.levels),
            )

        return get_serializer().serialize(data=mapping, api_metadata=api_metadata)

    return metadata_api, hierarchy_api


def register_metadata_apis(
    app, entities, metadata_schema, url_prefix="metadata", api_metadata=[]
):
    """Given an entity class, generate an API handler and register URL routes
    with flask. """

    api_metadata = {x: app.config[x] for x in api_metadata}

    for entity_name, settings in entities.items():

        # Generate handler function for entity
        # Get entity-specific schema if specified by user
        metadata_schema = settings.get("schema", metadata_schema)
        metadata_api_func, hierarchy_api_func = make_metadata_api(
            settings["classification"], metadata_schema, api_metadata
        )

        # Singular endpoint e.g. /entity/7
        app.add_url_rule(
            "/{url_prefix}/{entity_name}/<int:entity_id>".format(
                entity_name=entity_name, url_prefix=url_prefix
            ),
            endpoint=entity_name + "_singular",
            view_func=metadata_api_func,
        )

        # List endpoint e.g. /entity/
        app.add_url_rule(
            "/{url_prefix}/{entity_name}/".format(
                entity_name=entity_name, url_prefix=url_prefix
            ),
            endpoint=entity_name + "_all",
            view_func=metadata_api_func,
            defaults={"entity_id": None},
        )

        # Hierarchy endpoint e.g. /entity/hierarchy?from_level=blah&to_level=blah2
        app.add_url_rule(
            "/{url_prefix}/{entity_name}/hierarchy".format(
                entity_name=entity_name, url_prefix=url_prefix
            ),
            endpoint=entity_name + "_hierarchy",
            view_func=hierarchy_api_func,
        )

    return app
