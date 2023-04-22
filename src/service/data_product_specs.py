"""
Defines active data products in the service.

To add a data product to the service, create a new module in the data_products directory
and define a DataProductSpec. Add the import path and DPS variable name to the _SPECS variable.
The data product routes will be added to the OpenAPI UI in the same order as the first
item in the `tags` field of the router.

Note that "builtin" is a reserved ID for data products.
"""

# NOTE: Once a collection has been saved with a data product, the data product cannot be
# removed from the service without breaking that collection.

import importlib

from src.service.data_products.common_models import DataProductSpec

_SPECS = {
    "src.service.data_products.taxa_count": "TAXA_COUNT_SPEC",
    "src.service.data_products.genome_attributes": "GENOME_ATTRIBS_SPEC",
    "src.service.data_products.microtrait": "MICROTRAIT_SPEC",
}

_IMPORT_CACHE = {}  # load at app startup to prevent import loops


def _ensure_cache():
    if not _IMPORT_CACHE:
        for mod, spec in _SPECS.items():
            # just throw the errors for now, maybe do a bit better error handling if needed later
            loaded = getattr(importlib.import_module(mod), spec)
            if loaded.data_product in _IMPORT_CACHE:
                raise ValueError(f"> 1 data product with ID {loaded.data_product}")
            _IMPORT_CACHE[loaded.data_product] = loaded


def get_data_product_spec(data_product: str) -> DataProductSpec:
    """
    Get a spec for a data product.
    """
    _ensure_cache()
    if data_product not in _IMPORT_CACHE:
        raise ValueError(f"No such data product: {data_product}")
    return _IMPORT_CACHE[data_product]


def get_data_product_names() -> list[str]:
    """
    Get the sorted list of names of installed data products.
    """
    _ensure_cache()
    return sorted(_IMPORT_CACHE.keys())


def get_data_products() -> list[DataProductSpec]:
    """
    Get the list of installed data products in no particular order.
    """
    _ensure_cache()
    return list(_IMPORT_CACHE.values())
