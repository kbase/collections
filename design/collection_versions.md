# Collection versions

## Document purpose

Outline options for how to apply versions to KBase collections, and eventually record a decision.

## Prerequisites / background

Read the [core collections operations document](./core_collection_operations.md)

## Notes

This discussion only affects the collection version, not the load versions, which are
still expected to be arbitrary strings (and don't necessarily need to be exposed in the API).

## Options

### Arbitrary version tag only

* The user creating a version provides an arbitrary string tag for the version.
* Any semantics in the tag are up to the user.
* This is the original proposal from the core collections operations document.
* The versions could be ordered either by the natural order of the tags (which is likely not
  meaningful if the tags contain numbers) or the creation date of the version.

### Service supplied integer version with potential gaps

* The service keeps a counter for each collection and increments the counter each time a
  version is saved, applying the counter to the new version.
* The highest version *is not necessarily the active version and does not become active
  automatically*.
* The benefits over using just the creation date in epoch milliseconds, which is also a
  monotonically increasing integer, are:
  * Smaller numbers are easier to remember and discuss.
  * If versions are added very quickly they may have the same epoch value.
* A failure occurring between incrementing the counter and saving the document would result in
  a missing version in the sequence.
  * This is not particularly harmful other than presentation, as the version will simply not
    appear in a list of versions (unlike other KBase systems where the consequences are more dire).
  * A transaction may alleviate some of this issue, but adds more code complexity and
    [transactions are only reliable in non-sharded environments](https://github.com/arangodb/arangodb/issues/11424).

### Service supplied integer without gaps

* As above, but the service uses the same strategy as the Sample Service to ensure there are no
  version gaps.
* Adds considerable complexity to the code, including a periodic cleanup task and handling
  cases where the version update did not fully complete.

### Semantic versioning

* A semantic version would be applied to a collection version on creation.
* The user would tell the service whether to increment the major, minor, or patch version (MMPV)
  when creating the version.
* In a somewhat similar manner to the integer version, the service would keep a semantic version
  counter that records the most recent MMPV for each collection.
  * It's not clear how difficult the update would be, although AQL is very flexible. Would
    require more investigation if we wish to pursue this option.
* Similarly to the integer version, missing versions are possible.
* The Sample Service strategy only works for a monotonic integer, and so could not be applied here.
* In order for the versions to be ordered meaningfully, the version would have to be split into
  major, minor, and patch fields in the database, and all indexes would need to take that into
  account.
  * E.g. to sort a collection by versions, the 2 member compound index
   `(collection_id, load_version)` would have to change to a 4 member
   `(collection_id, major_version, minor_version, patch_version)` compound index.

### Arbitrary tag and integer version

* The service accepts a user supplied tag and also assigns an integer version as previously
  described.
  * The integer version could use either the counter strategy or the Sample Service strategy.
* This allows for memnonic tags assigned by the user as well as a clear ordering that is
  easier to understand, remember, and discuss than epoch time stamps.
* This will mean extra endpoints or parameters to allow for operations to specify a version by
  either the tag or the integer.

## Decision

22/10/24: Use an arbitrary tag and an integer version per the RE team.
