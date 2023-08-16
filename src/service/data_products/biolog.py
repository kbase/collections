"""
The Biolog data product.
"""

import src.common.storage.collection_and_field_names as names
from src.service.data_products.heatmap import HeatMapController

_BIOLOG_CONTROLLER = HeatMapController(
    "biolog",
    "Biolog",
    names.COLL_BIOLOG_META,
    names.COLL_BIOLOG_DATA,
    names.COLL_BIOLOG_CELLS,
)

BIOLOG_SPEC = _BIOLOG_CONTROLLER.get_data_product_spec()
