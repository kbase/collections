"""
Defines active data products in the service.

To add a data product to the service, create a new module in the data_products directory
and define a DataProductSpec. Import it in this file and add it to the DATA_PRODUCTS variable.
The data product routes will be added to the OpenAPI UI in the same order as the first
item in the `tags` field of the router.

Note that "builtin" is a reserved ID for data products.
"""

from src.service.data_products.common_models import DataProductSpec
from src.service.data_products import taxa_count, genome_attributes


DATA_PRODUCTS: list[DataProductSpec] = [
    taxa_count.TAXA_COUNT_SPEC,
    genome_attributes.GENOME_ATTRIBS_SPEC
]
""" Set of data products the service supports. """


# TODO DATAPROD add markdown docs explaining how to create data products
