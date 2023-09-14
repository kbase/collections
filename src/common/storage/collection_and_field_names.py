"""
This file documents database collection and field names used by the service and loaders,
as both will need to communicate using the same collections and fields within the collections.
Service-exclusive collection names are also listed here to prevent name collisions.

In text, a capitalized Collections refers to the Collections service and loaders, while lowercase
refers to the database collections. However, all collection names are lowercase.

Variables holding ArangoDB collection names must be annotated appropriately - see the collection
variables below for examples.
"""

from typing import Annotated
from src.common.storage.field_names import *  # @UnusedWildImport

# collection variables should be prefixed with COLL_, fields prefixed with FLD_.

# Global fields

## These fields aren't specific to a particular collection. collection specific fields are 
## specified along with their collection.

FLD_ARANGO_KEY = "_key"
FLD_ARANGO_ID = "_id"

FLD_COLLECTION_ID = "coll"
""" The name of the key that has a collection ID as its value. """

FLD_LOAD_VERSION = "load_ver"
""" The name of the key that has a load version as its value. """

FLD_DATA_PRODUCT = "data_product"
""" The name of the key that has a data product ID as its value. """

FLD_INTERNAL_ID = "internal_id"
""" The name of the key that has an internal match ID as its value. """

FLD_TYPES = "types"
""" The name of the key that has a list of workspace types as its value. """

FLD_UPA_MAP = "_upas"
"""
The name of the key that has a mapping of workspace type to workspace UPA for workspace
data associated with the document.
"""

FLD_MATCHES_SELECTIONS = "_mtchsel"
"""
Used for marking matches and selections; contains a list of internal match or selection IDs.
Underscore to separate from "real" attribs
"""

FLD_MATCHED = "match"
"""
Used for marking matches when returning data to the user and they request match marking vs.
filtering on a match.
"""

FLD_MATCHED_SAFE = "__match__"
"""
Used for marking matches when returning data to the user and they request match marking vs.
filtering on a match, and the standard match field can't be used since the keys in the
returned data are undefined.
"""

FLD_SELECTED = "sel"
"""
Used for marking selections when returning data to the user and they request selection marking vs.
filtering on a selection.
"""

FLD_SELECTED_SAFE = "__sel__"
"""
Used for marking selections when returning data to the user and they request selection marking vs.
filtering on a selection, and the standard selection field can't be used since the keys in the
returned data are undefined.
"""

# Collections

COLL_ANNOTATION = "ArangoDB collection name"
"""
ArangoDB collection name constants are annotated with this value in index 0 and metadata about
the collection in index 1.
See examples below.
"""

COLL_ANNOKEY_DESCRIPTION = "desc"
"""
A collection metadata key. The value should be the description of the metadata.
"""


COLL_ANNOKEY_SUGGESTED_SHARDS = "suggshards"
"""
A collection metadata key. The value should be the suggested number of shards for the collection.
"""

COLLECTION_PREFIX = "kbcoll_"
"""
The prefix for all Collections database collection names. Since the service and loaders
are expected to operate in a database shared with several other services, this prefix
provides a namespace for the exclusive use of the Collections code.
"""

## Collection service exclusive collections

_SRV_PREFIX = COLLECTION_PREFIX + "coll_"
# The namespace for service exclusive collections

COLL_SRV_CONFIG: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION:
            "A collection holding dynamic configuration data for the collections service.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 1,
    }
] = _SRV_PREFIX + "config"

COLL_SRV_COUNTERS: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding counters for Collection versions.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 1,
    }
] = _SRV_PREFIX + "counters"

COLL_SRV_VERSIONS: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding Collection versions.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 1,
    }
] = _SRV_PREFIX + "versions"

COLL_SRV_ACTIVE: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding active Collections.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 1,
    }

] = _SRV_PREFIX + "active"

COLL_SRV_MATCHES: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding matches to Collections.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 3,
    }
] = _SRV_PREFIX + "matches"

COLL_SRV_MATCHES_DELETED: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding matches in the deleted state.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 1,
    }
] = COLL_SRV_MATCHES + "_deleted"

COLL_SRV_DATA_PRODUCT_PROCESSES: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION:
            "A collection holding the status of calculating matches and selections.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 3,
    }
] = _SRV_PREFIX + "data_prod_proc"

COLL_SRV_SELECTIONS: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding selections for Collections.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 3,
    }
] = _SRV_PREFIX + "selections"

COLL_SRV_SELECTIONS_DELETED: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding selections in the deleted state.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 1,
    }
] = COLL_SRV_SELECTIONS + "_deleted"

## Non-data product specific collection shared between loaders and service

COLL_EXPORT_TYPES: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION:
            "A collection holding types available for export from specific data products",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 1,
    }
] = COLLECTION_PREFIX + "export_types"

## Data product collections

### Taxa counts

COLL_TAXA_COUNT_RANKS: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding taxa count rank data.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 1,
    }
] = COLLECTION_PREFIX + "taxa_count_ranks"

COLL_TAXA_COUNT: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding taxa count data.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 3,
    }
] = COLLECTION_PREFIX + "taxa_count"

#### Taxa count document fields
FLD_TAXA_COUNT_RANK = "rank"
FLD_TAXA_COUNT_RANKS = "ranks"
FLD_TAXA_COUNT_NAME = "name"
FLD_TAXA_COUNT_COUNT = "count"

### Genome attributes

GENOME_ATTRIBS_PRODUCT_ID = "genome_attribs"

COLL_GENOME_ATTRIBS_META: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding genome attributes metadata.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 1,
    }
] = COLLECTION_PREFIX + GENOME_ATTRIBS_PRODUCT_ID + "_meta"

COLL_GENOME_ATTRIBS: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding genome attributes data.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 3,
    }
] = COLLECTION_PREFIX + GENOME_ATTRIBS_PRODUCT_ID

#### Genome attribute document fields

# Used for lineage matchers
FLD_GENOME_ATTRIBS_GTDB_LINEAGE = "classification"

### Heatmap general fields for heat map data products (e.g. microTrait)

FLD_HEATMAP_COLUMN_CATEGORIES = "categories"
# the categories field for the column data document. The structure of the document is defined
# in /src/common/product_models/heatmap_common_models.py

### microTrait

_MICROTRAIT_COLL_PREFIX = COLLECTION_PREFIX + "microtrait_"

COLL_MICROTRAIT_META: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding microtrait heatmap metadata.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 1,
    }
    
] = _MICROTRAIT_COLL_PREFIX  + "meta"

COLL_MICROTRAIT_DATA: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding microtrait heatmap data.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 3,
    }
] = _MICROTRAIT_COLL_PREFIX + "data"

COLL_MICROTRAIT_CELLS: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding microtrait heatmap cell detail data.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 3,
    }
] = _MICROTRAIT_COLL_PREFIX + "cells"


### samples

COLL_SAMPLES: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding sample data.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 3,
    }
    
] = COLLECTION_PREFIX + "samples"

FLD_SAMPLE_LATITUDE = "latitude"
""" Key name for latitude data in degrees"""

FLD_SAMPLE_LONGITUDE = "longitude"
""" Key name for longitude data in degrees"""

FLD_SAMPLE_GEO = '_geo_spatial'
""" Key name for sample geo-spatial data in format of [longitude, latitude] """

FLD_KB_SAMPLE_ID = "kbase_sample_id"
""" Key name for KBase sample id """


### biolog

_BIOLOG_COLL_PREFIX = COLLECTION_PREFIX + "biolog_"

COLL_BIOLOG_META: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding Biolog heatmap metadata.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 1,
    }
    
] = _BIOLOG_COLL_PREFIX  + "meta"

COLL_BIOLOG_DATA: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding Biolog heatmap data.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 3,
    }
] = _BIOLOG_COLL_PREFIX + "data"

COLL_BIOLOG_CELLS: Annotated[
    str,
    COLL_ANNOTATION,
    {
        COLL_ANNOKEY_DESCRIPTION: "A collection holding Biolog heatmap cell detail data.",
        COLL_ANNOKEY_SUGGESTED_SHARDS: 3,
    }
] = _BIOLOG_COLL_PREFIX + "cells"
