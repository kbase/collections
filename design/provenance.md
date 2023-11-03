# Provenance design notes

## Background

* When a Assembly or Genome set (or other types in the future) is saved, we wish to add
  provenance information to the saved object.
* These design notes are for the first version of provenance handling. We expect to improve upon it
  in the future.
  
## Design

* Add a text box that allows a user to add an arbitrary description of the set. This description
  will be added to the description field in the set object.
  * Note that the description field is an auto meta field in the `KBaseSets` set types,
    and therefore the description is
    [limited to 800 bytes](https://ci.kbase.us/services/ws/docs/limits.html).
  * All of the currently supported set types have a top level field:
    * [KBaseSearch.GenomeSet](https://narrative.kbase.us/#spec/type/KBaseSearch.GenomeSet)
      * Evetually supposed to be replaced by
        [KBaseSets.GenomeSet](https://narrative.kbase.us/#spec/type/KBaseSets.GenomeSet)
    * [KBaseSets.AssemblySet](https://narrative.kbase.us/#spec/type/KBaseSets.AssemblySet)
* In the provenance data structure add relevant fields as per the example below:

```
{
  "epoch": <timestamp>,
  "service": "collections",
  "service_ver": <the version of the collections service>,
  "method": "toset",
  "method_params": [<match parameters, if any>],
  "custom": {
    "collection": <collection name>,
    "collection_version": <collection integer version as string (1)>,
    "matcher_id": <matcher ID if applicable>,
    "selection_modified_post_match": <boolean as string>
  },
  "description": "A set saved by the KBase Collections Service from the user's
    selection <created from a match with the provided parameters if applicable> 
}
(1) Note that collection versions are ephemeral and may be replaced at any time, but
knowing the version can aid in debugging.
```

* Note that the provenance as a whole can be no more than 1MB. Since matches can be
  no more  than 10k UPAs this is probably ok.
* For the match parameters to be provided, the UI will need to send the match ID to the server
  with the request.
* Note that it's possible for a user to create a match, create a selection from a match, and
  then alter the selection. Should the match information be included in that case? Should we
  note that a match was involved but is not an exact representation of the set? If so, how?
  * The `selection_modified_post_match` is one possibility for how to manage this and could
    be figured out server side given the match and selection IDs.


## Potential future improvements

* Keep any set UPAs in the match and add them to the match parameters in the provenance.
  * Currently sets are unpacked and "discarded" in the backend.