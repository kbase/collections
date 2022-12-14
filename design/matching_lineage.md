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

* We assume tha the matching computation should happen in the CS.
  * The matching algorithm will need to be able to query ArangoDB to find matches
  * Apps should probably not have query access to ArangoDB
* General outline
  * User provides a list of G/As or a G/A Set to the collections service as UPAs
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
    * **Question**: What happens if GTDB is updated and a user submits genomes with earlier
      lineages?
  * Exact matches:
    * The CS runs an `IN` AQL query on the collection attributes AC to find matching members of
      the collection
      * May need to be split into multiple queries if there are too many inputs
    * This may return tens of thousands of matches per input G/A
    * The match inputs and outputs are stored in an Arango document or documents and a match ID
      returned to the user
  * Rank matches:
    * The user must also provide a rank level (e.g. phylum, domain, etc)
    * The CS runs an `IN` AQL query on the collection attributes AC to find matching members of
      the collection
      * May need to be split into multiple queries if there are too many inputs
    * The CS truncates the first stage matches' lineages to the provided rank
    * The CS runs an `OR` query with a stanza for each truncated lineage with a
      [starts with](https://github.com/arangodb/arangodb/issues/1796)
      query to find all the members in the subtree
      * An index on the lineage should still be able to support the starts with query
        * **TODO**: test this assumption
      * This might be pretty slow. We might have to do some experiements with data schemas and
        queries.
    * This may return tens of thousands of matches per input G/A
    * The match inputs and outputs are stored in an Arango document or documents and a match ID
      returned to the user
  * Nearest matches:
    * TBD. Don't have a clear understanding of this yet
  * Matches
    * Matches are not sharable and are ephemeral. They expire after X hours (X = 24?).
      * Will need some means of cleaning up the match and any supporting DP data produced (see
        below), including ArangoDB records, cache service data, etc.
  * Data visualization
    * When accessing a DP (like `taxa_counts`) the user would provide the match ID
    * The DP is responsible for any processing that is required to support the visualization,
      including caching
    * For instance, the collection attributes DB might pull the matched subset into a separate
      collection to allow for fast sorting / filtering without having to send a potentially
      enormous `IN` query to the regular collection