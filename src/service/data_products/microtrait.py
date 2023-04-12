"""
The microTrait data product.
"""

import src.common.storage.collection_and_field_names as names
from src.service.data_products.heatmap import HeatMapController

_MICROTRAIT_CONTROLLER = HeatMapController(
    "microtrait",
    "microTrait",
    names.COLL_MICROTRAIT_META,
    names.COLL_MICROTRAIT_DATA,
    names.COLL_MICROTRAIT_CELLS,
)

MICROTRAIT_SPEC = _MICROTRAIT_CONTROLLER.get_data_product_spec()
