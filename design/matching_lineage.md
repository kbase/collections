# Collections Taxonomy Lineage Matching

## Document purpose

Propose how taxonomy lineage matching might work WRT the KBase Collections project.

## Nomenclature

AC - ArangoDB Collection, as opposed to KBase collection  
CS - Collections Service  
DP - collections Data Product  
G/A - Genome / Assembly  
RE - Relation Engine  
UPA - the Unique Permanent Address of a workspace object  
WS - Workspace Service  

## Matching

* The matching computation occurs in the CS.
* General outline
  * User provides a list of G/As or a G/A Set to the collections service as UPAs
    * Multiple workspaces are allowed
    * User must have read permissions to the workspaces and the objects must not be deleted or
      missing
  * The CS pulls the object metadata for each individual Genome or Assembly from the WS
    * The objects are expected to have GTDB lineage information in the metadata
      * Otherwise pulling the objects themselves to get the lineage will take a really long time
      * In the future perhaps `GTDB_tk` could be run automatically on imported G/As and
        stored either in the WS object or in the RE shadow workspace object
    * If the objects are missing lineage data CS throws an error and the user is advised to run
      `GTDB_tk` on their genomes
      * This requires the Genome and Assembly spec to be updated to add the lineage information
        to the WS metadata and `GTDB_tk` to be updated to add the lineage information
      * In the future the CS may be able to run `GTDB_tk` on behalf of the user, although that
        means we will need some sort of async job system to manage matching
        * The results might overwrite the old WS G/A object with a new object, or
          store the lineage on the RE shadow workspace object
  * When a match is accessed, either directly for via a DP:
    * If the match input object permissions for the user haven't been checked after X minutes,
      the service must recheck, as workspace permissions can change and objects and workspaces
      can be deleted.
      * X may be hardcoded, settable in the service config, or dynamically settable via an API. TBD
    * If any of the data product load versions have changed, the match much be immediately
      expired.
      * As such, the current load versions must be stored with the match.
  * General matching
    * After completing a match, the match inputs and outputs are stored in an Arango document
      or documents and a hash of the data returned to the user as a match ID
      * A hash based ID means a rematch is not required for the same inputs while the match
        exists in the system, regardless of device, browser, or user
      * A UUID is attached to the match to use to identify secondary data storage
        (for DPs, for example)
      * The UUID is strictly internal only and should not be visible in the API.
        * This allows for atomic match deletion. Consider the following scenario:
          * A match is created and the hash ID returned
          * It's not used for a while and deletion of secondary data starts (which could take a
            while)
          * The same match is run again, and therefore the same hash ID returned
          * A DP is accessed, which uses the match ID to look up secondary data and
            either fails or returns incomplete data as it's mid deletion
        * With a UUID for secondary data, the original match can be deleted atomically, and any
          new match will get a new UUID, and so a DP will know it needs to recreate the data
    * Matches are not expected to be searchable or otherwise accessible unless the user has
      the match ID.
  * Exact matches:
    * The CS runs an `IN` AQL query on the collection attributes AC to find matching members of
      the collection
      * May need to be split into multiple queries if there are too many inputs
    * This may return tens of thousands of matches per input G/A
  * Rank matches:
    * The user must also provide a rank level (e.g. phylum, domain, etc)
    * The CS truncates the input matches' lineages to the provided rank
    * The CS runs an `OR` query with a stanza for each truncated lineage with a
      [starts with](https://github.com/arangodb/arangodb/issues/1796)
      query to find all the members in the subtree
      * An index on the lineage should still be able to support the starts with query
        * **TODO**: test this assumption
      * This might be pretty slow. We might have to do some experiements with data schemas and
        queries.
    * This may return tens of thousands of matches per input G/A
  * Nearest matches:
    * TBD. Don't have a clear understanding of this yet
  * Matches
    * Matches are not sharable and are ephemeral. They expire after X hours (X = 24?).
      * X could be hardcoded, configured in the service configuration, or dynamically settable
        via an API. TBD
      * Matches must expire based on the *last usage* - e.g. if a match is retrieved, its lifetime
        must be renewed.
        * Perhaps up to some maximum lifetime, to avoid crafy people setting up cron jobs to
          touch their matches.
      * Will need some means of cleaning up the match and any supporting DP data produced (see
        below), including ArangoDB records, cache service data, etc.
  * Data visualization
    * When accessing a DP (like `taxa_counts`) the user would provide the match ID
    * The DP is responsible for any processing that is required to support the visualization,
      including caching
    * For instance, the collection attributes DB might pull the matched subset into a separate
      collection to allow for fast sorting / filtering without having to send a potentially
      enormous `IN` query to the regular collection
* Connectors
  * Connectors represent a means of matching incoming data to a collection, and are responsible
    for defining the inputs for a match (one of which is always a list of UPAs), performing the
    match, and returning the result in a standard format.
    * Connectors are also responsible for checking that match parameters are valid - for instance,
      if the GTDB lineage version of the input data is not the same as the collection version,
      the connector may throw an error or take some other action to check if the match can
      proceed and be valid
      * NOTE: this implies that for non-GTDB collections we need to label it with a GDTB
        version somehow if GTDB lineage matching is supported
        * Alternative - support connector parameters that are stored in the collections object, and
          make GTDB version a connector parameter
    * Connectors must define which workspace object types they accept.
    * There should be a connectors endpoint for a collection that lists what connectors are
      available and the input parameters (other than the ubiquitous UPA list)