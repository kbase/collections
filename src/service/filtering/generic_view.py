from src.common.product_models.columnar_attribs_common_models import ColumnarAttributesSpec, AttributesColumnSpec

_FIELD_COL = {
    "kbase_display_name": {
        "type": "string",
        "filter_strategy": "ngram",
    }
}


def create_generic_spec() -> ColumnarAttributesSpec:
    """
    Create a generic spec for a collection.

    Arango search includeAllFields will be set to true. This means that all fields will be indexed
    by default except for those that are explicitly defined in the _FIELD_COL dict.
    """

    columns = [AttributesColumnSpec(key=key, **specs) for key, specs in _FIELD_COL.items()]

    return ColumnarAttributesSpec(columns=columns)
