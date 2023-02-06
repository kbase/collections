"""
Convert data structures to documents suitable for saving in a document-based database like
MongoDB or ArangoDB.
"""

from src.common.gtdb_lineage import TaxaNodeCount, GTDB_RANK_ABBREV_TO_FULL_NAME
from src.common.hash import md5_string
from src.common.storage import collection_and_field_names as names


def taxa_node_count_to_doc(
    kbase_collection: str,
    load_version: str,
    taxa_count: TaxaNodeCount,
    internal_match_id: str = None
) -> dict[str, str | int]:
    """
    Convert a taxa node count to a document suitable for storage in a document based database.

    kbase_collection - the name of the KBase collection with which the data is associated
    load_version - the load version of the data set
    taxa_count - the taxa count information to convert
    internal_match_id - the internal match ID of the related match, if any
    """
    full_rank = GTDB_RANK_ABBREV_TO_FULL_NAME[taxa_count.rank]
    match_id_str = f"{internal_match_id}_" if internal_match_id else ""
    doc = {
        names.FLD_ARANGO_KEY: md5_string(
            f"{kbase_collection}_{load_version}_{match_id_str}_{full_rank}_{taxa_count.name}"
        ),
        names.FLD_COLLECTION_ID: kbase_collection,
        names.FLD_LOAD_VERSION: load_version,
        names.FLD_TAXA_COUNT_RANK: full_rank,
        names.FLD_TAXA_COUNT_NAME: taxa_count.name,
        names.FLD_TAXA_COUNT_COUNT: taxa_count.count,
        names.FLD_INTERNAL_MATCH_ID: internal_match_id,
    }
    return doc
