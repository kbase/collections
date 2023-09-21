"""
Load one or more collection product specs into a data structure. The spec provides the type,
and for some types, the filtering strategy, for each user-visible field for a particular KBase
collection (e.g. PMI) and data product (e.g. genome_attribs).

The specs are stored in the same directory as this module.

When a tool is updated to output different or changed fields, it's expected that the spec that
contains those fields is updated as well.
"""
import yaml
from pathlib import Path
from src.common.product_models.columnar_attribs_common_models import ColumnarAttributesSpec


SPEC_DIR = Path(__file__).parent.resolve()
YML_SUFFIX = ".yml"


def load_spec(data_product: str, collection: str = None) -> ColumnarAttributesSpec:
    """
    Load a collection product spec into a data structure.
    
    data_product - the data product (e.g. genome_attribs) for which to retrieve the spec.
    collection - the collection (e.g. GROW) for which to retrieve the spec. If the collection is
        omitted, all specs for that collection are merged together, throwing an error if keys
        conflict.
    """
    if not collection:
        raise ValueError("I ain't done this yet, dang")
    # TODO FILTER load multiple specs and merge them, throwing errors on keys with different specs.
    with open(SPEC_DIR / f"{data_product}-{collection}{YML_SUFFIX}") as f:
        return ColumnarAttributesSpec(**yaml.safe_load(f))
