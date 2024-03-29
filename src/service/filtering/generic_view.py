from src.common.product_models.columnar_attribs_common_models import ColumnarAttributesSpec, AttributesColumnSpec

_FIELD_COL = {
    "kbase_display_name": {
        "type": "string",
        "filter_strategy": "ngram",
        "non_visible": True,
    }
}

_GENERIC_VIEW_NAME_SUFFIX = "_generic_view"


def create_generic_spec() -> ColumnarAttributesSpec:
    """
    Create a generic column spec for a collection.

    This spec only includes fields common to most or all collections that require special handling and it is
    expected that all other fields are handled in a generic way.
    """

    columns = [AttributesColumnSpec(key=key, **specs) for key, specs in _FIELD_COL.items()]

    return ColumnarAttributesSpec(columns=columns)


def get_generic_view_name(data_product: str) -> str:
    """
    Get the name of the generic view for a data product.
    """
    return f"{data_product}{_GENERIC_VIEW_NAME_SUFFIX}"
