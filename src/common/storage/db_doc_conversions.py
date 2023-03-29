"""
Convert data structures to documents suitable for saving in a document-based database like
MongoDB or ArangoDB.
"""

from src.common.gtdb_lineage import TaxaNodeCount
from src.common.hash import md5_string
from src.common.storage import collection_and_field_names as names


def taxa_node_count_to_doc(
    kbase_collection: str,
    load_version: str,
    taxa_count: TaxaNodeCount,
    internal_id: str = None
) -> dict[str, str | int]:
    """
    Convert a taxa node count to a document suitable for storage in a document based database.

    kbase_collection - the name of the KBase collection with which the data is associated
    load_version - the load version of the data set
    taxa_count - the taxa count information to convert
    internal_id - the internal ID of a related match or selection, if any
    """
    full_rank = taxa_count.rank.value
    id_str = f"{internal_id}_" if internal_id else ""
    return {
        names.FLD_ARANGO_KEY: md5_string(
            f"{kbase_collection}_{load_version}_{id_str}_{full_rank}_{taxa_count.name}"
        ),
        names.FLD_COLLECTION_ID: kbase_collection,
        names.FLD_LOAD_VERSION: load_version,
        names.FLD_TAXA_COUNT_RANK: full_rank,
        names.FLD_TAXA_COUNT_NAME: taxa_count.name,
        names.FLD_TAXA_COUNT_COUNT: taxa_count.count,
        names.FLD_INTERNAL_ID: internal_id,
    }


def data_product_export_types_to_doc(
    kbase_collection: str, data_product: str, load_version: str, types: list[str]
) -> dict[str, str | list[str]]:
    """
    Create a document for a set of export types available for a data product.

    kbase_collection - the name of the KBase collection with which the data is associated
    data_product - the ID of the data product, e.g. `genome_attribs`
    load_version - the load version of the data set
    types - the list of workspace types that are exportable from the data product, e.g.
        `KBaseGenomes.Genome`, `KBaseGenomeAnnotations.Assembly`
    """
    return {
        names.FLD_ARANGO_KEY: md5_string(f"{kbase_collection}_{data_product}_{load_version}"),
        names.FLD_COLLECTION_ID: kbase_collection,
        names.FLD_DATA_PRODUCT: data_product,
        names.FLD_LOAD_VERSION: load_version,
        names.FLD_TYPES: types,
    }

