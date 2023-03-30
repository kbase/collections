"""
This file documents database collection and field names used by the service and loaders,
as both will need to communicate using the same collections and fields within the collections.
Service-exclusive collection names are also listed here to prevent name collisions.

In text, a capitalized Collections refers to the Collections service and loaders, while lowercase
refers to the database collections. However, all collection names are lowercase.
"""

# collection variables should be prefixed with COLL_, fields prefixed with FLD_.

# Global fields

## These fields aren't specific to a particular collection. collection specific fields are 
## specfied along with their collection.

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

# Collections

COLLECTION_PREFIX = "kbcoll_"
"""
The prefix for all Collections database collection names. Since the service and loaders
are expected to operate in a database shared with several other services, this prefix
provides a namespace for the exclusive use of the Collections code.
"""

## Collection service exclusive collections

_SRV_PREFIX = COLLECTION_PREFIX + "coll_"
# The namespace for service exclusive collections

COLL_SRV_COUNTERS = _SRV_PREFIX + "counters"
""" A collection holding counters for Collection versions. """

COLL_SRV_VERSIONS = _SRV_PREFIX + "versions"
""" A collection holding Collection versions. """

COLL_SRV_ACTIVE = _SRV_PREFIX + "active"
""" A collection holding active Collections. """

COLL_SRV_MATCHES = _SRV_PREFIX + "matches"
""" A collection holding matches to Collections. """

COLL_SRV_MATCHES_DELETED = COLL_SRV_MATCHES + "_deleted"
""" A collection holding matches in the deleted state. """

COLL_SRV_DATA_PRODUCT_PROCESSES = _SRV_PREFIX + "data_prod_proc"
""" A collection holding the status of calculating secondary data products for matches. """

COLL_SRV_SELECTIONS = _SRV_PREFIX + "selections"
""" A collection holding selections for Collections. """

COLL_SRV_SELECTIONS_DELETED = COLL_SRV_SELECTIONS + "_deleted"
""" A collection holding selections in the deleted state. """

## Non-data product specific collection shared between loaders and service

# Types available for export from specific data products
COLL_EXPORT_TYPES = COLLECTION_PREFIX + "export_types"

## Data product collections

### Taxa counts

COLL_TAXA_COUNT_RANKS = COLLECTION_PREFIX + "taxa_count_ranks"
""" A collection holding taxa count rank data. """

COLL_TAXA_COUNT = COLLECTION_PREFIX + "taxa_count"
""" A collection holding taxa count data. """

#### Taxa count document fields
FLD_TAXA_COUNT_RANK = "rank"
FLD_TAXA_COUNT_RANKS = "ranks"
FLD_TAXA_COUNT_NAME = "name"
FLD_TAXA_COUNT_COUNT = "count"

### Genome attributes

COLL_GENOME_ATTRIBS = COLLECTION_PREFIX + "genome_attribs"

#### Genome attribute document fields

# Used as sort key in genome attributes collection
FLD_GENOME_ATTRIBS_KBASE_GENOME_ID = "kbase_genome_id" 

# Used for lineage matchers
FLD_GENOME_ATTRIBS_GTDB_LINEAGE = "classification"

# Used for marking matches and selections in the Arango collection; contains a list of internal
# match or selection IDs.
# Underscore to separate from "real" attribs
FLD_GENOME_ATTRIBS_MATCHES_SELECTIONS = "_mtchsel"

# Used for marking matches when returning data to the user and they select match marking vs.
# filtering on a match.
FLD_GENOME_ATTRIBS_MATCHED = "__match__"

# Used for marking selections when returning data to the user and they select selection marking vs.
# filtering on a selectin.
FLD_GENOME_ATTRIBS_SELECTED = "__sel__"
