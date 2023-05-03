# Matcher set design document

## Document purpose

Outline the design of matcher sets in the KBase collections project, which allow a user to combine
the results of one or more matchers when viewing data in the service.

## Nomenclature

UPA - the Unique Permanent Address of a KBase Workspace object.

## General Design

A matcher set is a combination of one or more KBase collections matchers.

* A matcher set with one matcher is essentially equivalent to a single matcher, but we allow
  a single matcher in a set to unify the matcher interface.
* When a user creates a matcher set, they specify one or more individual matchers as normal,
  but the match only applies to a single set of input UPAs, collection and version of said
  collection.
* When sent to the service, the matcher set results in the creation of
  * An individual match database record and, potentially, process for each matcher in the set
  * A match set database record containing the IDs of the individual matchers
* The ID of the set record is returned to the client along with
  * The current states of the individual matches
  * The overall state of the match set, which is the least favorable state from the individual
    matchers, with the precedence `failed` > `processing` > `complete`
* Otherwise the match set behaves like a single match, and the ID is fungible with a single
  match ID everywhere in the service API, with the union of the individual matches treated as the
  match set.
* A match set will be deleted via the same rules as an individual match.
* A match component of a match set cannot be deleted before the match set, as accessing a
  match set will update the access time of the match set before updating the access time of
  the individual match. 

## Design benefits

* If a user wishes to examine a subset of a match set, a new match set containing the matcher
  subset is very cheap to make, as the matches are already complete.
* Similarly, users can mix individual matchers from match sets into new match sets cheaply.
* The above points include matches originally created by other users, whether singly or as part
  of a set.

## API changes

* The current single matcher endpoint will be left as is as a convenience.
* A new authorization-required endpoint for matcher sets will be added:

```
POST /collections/{collection_id}/matcherset/
{
    "matchers": [
        {
            "matcher_id": <matcher ID>,
            "parameters": {<parameters go here>},

        }
        ...
    ],
    "upas": [<UPAS go here>]
}
RETURNS:
{
    "match_set_id": <match set ID>,
    "state": <match set state>,
    "collection_id": <collection ID>,
    "collection_ver": <collection version>,
    "matchers": [
        {
            "match_id": <match ID>,
            "matcher_id": <matcher ID>,
            "state": <matcher state>
            "user_parameters": {<user parameters go here>},
            "collection_parameters": {<collection parameters go here>}
        }
        ...
    ]
}
```

  * The `<match_set_id>` is the sorted, concatenated, and `md5`'d list of individual matcher IDs,
    prepended with `set:`.
* The match status endpoint will be changed **in a backwards incompatible way** to support both
  individual matches and match sets.
  * The endpoint doesn't change, but the new return structure is:
```
{
    "match_set_id": <the match set ID or null for a single match>
    "state": <match set state>,
    "collection_id": <collection ID>,
    "collection_ver": <collection version>,
    "upas": [<upas go here>],
    "matches": [
        {
            "match_id": <match ID>,
            "matcher_id": <matcher Id>,
            "state": <matcher state>
            "user_parameters": {<user parameters go here>},
            "collection_parameters": {<collection parameters go here>},
            "matches": [<matched data IDs go here>]
        }
        ...
    ]
}

```
* The heatmap missing IDs endpoint will be changed **in a backwards incompatible way** to support
  both individal matches and match sets.
  * The endpoint doesn't change, but the new return structure is:
```
{
    "heatmap_match_state": <match set state>,
    "heatmap_selection_state": <selection state>,
    "match_missing": [
        {
            "match_id": <match ID>,
            "state": <match state>
            "missing": [<missing IDs go here>]
        }
        ...
    ],
    "selection_missing": [<missing IDs go here>]
}
```
* Otherwise, existing endpoints behave the same way they do now.