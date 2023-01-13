"""
Defines active data products in the service.

To add a data product to the service, create a new module in the data_products directory
and define a DataProductSpec. Import it in this file and add it to the DATA_PRODUCTS variable.
The data product routes will be added to the OpenAPI UI in the same order as the first
item in the `tags` field of the router.

Note that "builtin" is a reserved ID for data products.
"""

# NOTE: Once a collection has been saved with a data product, the data product cannot be
# removed from the service without breaking that collection.

from src.service.data_products.common_models import DataProductSpec
from src.service.data_products.taxa_count import TAXA_COUNT_SPEC
from src.service.data_products.genome_attributes import GENOME_ATTRIBS_SPEC


DATA_PRODUCTS: dict[str, DataProductSpec] = {
    TAXA_COUNT_SPEC.data_product: TAXA_COUNT_SPEC,
    GENOME_ATTRIBS_SPEC.data_product: GENOME_ATTRIBS_SPEC,
}
""" Set of data products the service supports. """
