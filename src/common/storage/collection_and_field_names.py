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

# Collections

COLLECTION_PREFIX = "kbcoll_"
"""
The prefix for all Collections database collection names. Since the service and loaders
are expected to operate in a database shared with several other services, this prefix
provides a namespace for the exclusive use of the Collections code.
"""

DEFAULT_KBASE_COLL_NAME = 'GTDB'
"""
The Default kbase collection identifier name
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

FLD_GENOME_ATTRIBS_GENOME_NAME = "genome_name"