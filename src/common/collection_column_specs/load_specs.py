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
from src.common.product_models.columnar_attribs_common_models import (
    AttributesColumnSpec,
    ColumnarAttributesSpec,
)

SPEC_DIR = Path(__file__).parent.resolve()
YML_SUFFIX = ".yml"


def _filename(data_product: str, collection: str) -> str:
    return SPEC_DIR / f"{data_product}-{collection}{YML_SUFFIX}"


def _get_spec_files(data_product: str) -> list[str]:
    return list(SPEC_DIR.glob(f"{data_product}-*{YML_SUFFIX}"))


def _specfile_to_collection(spec_file: Path) -> str:
    return str(spec_file).split("-")[1][:-len(YML_SUFFIX)]


def get_collections_for_data_product(data_product: str):
    """
    Given a data product, get the list of collections that have specs for that data product.
    """
    return {_specfile_to_collection(f) for f in _get_spec_files(data_product)}
    

def load_spec(data_product: str, collection: str = None) -> ColumnarAttributesSpec:
    """
    Load a collection product spec into a data structure.
    
    data_product - the data product (e.g. genome_attribs) for which to retrieve the spec.
    collection - the collection (e.g. GROW) for which to retrieve the spec. If the collection is
        omitted, all specs for that collection are merged together, throwing an error if keys
        conflict.
    """
    if collection:
        coll2file = {collection: _filename(data_product, collection)}
    else:
        coll2file = {_specfile_to_collection(f): SPEC_DIR / f
                     for f in _get_spec_files(data_product)}
    if not coll2file:
        raise ValueError(f"No specs found for data product {data_product}")
    coll2spec = {}
    for coll, specfile in coll2file.items():
        with open(specfile) as f:
            coll2spec[coll] = {d["key"]: d for d in yaml.safe_load(f)["columns"]}
    keys = set()
    for v in coll2spec.values():
        keys |= v.keys()
    columns = []
    for k in keys:
        first = None
        for coll, spec in coll2spec.items():
            if k in spec:
                if not first:
                    first = (coll, spec[k])
                else:
                    if spec[k] != first[1]:
                        colls = sorted([coll, first[0]])
                        raise ValueError(f"Column spec conflict for data product {data_product}, "
                                        + f"collections {colls[0]} and {colls[1]} on key {k}")
        columns.append(AttributesColumnSpec(**first[1]))
    return ColumnarAttributesSpec(columns=columns, spec_files=coll2file.values())
