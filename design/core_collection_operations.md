# Design of core collection operations

This document describes an inital design of the core, general data collections operations - e.g.
operations that aren't specific to a particular collection.

## Assumptions

* Collection information can change at any time, and previous versions are not necessarily
  accessible.
  * They should be accessible, at least, to database admins, and possibly via admin API methods.
* Collections can theoretically be reverted to prior versions at any time.
* We expect < 1000 collections over the lifetime of the collections infrastructure.
* There may be many versions of collections as they are updated and adjusted over time.
  * Including adding new data products to collections.
* Data product data structures / UIs / etc for collections will usually, but not always,
  be applicable to other collections as well.

## Collection operations / versions

* Save a collection version.
  * Administration only operation.
  * Once saved, a collection version is immutable and cannot be overwritten.
    * Trying to save a collection with an existing version will result in an error.
  * A collection version *is not* immediately active.
    * At some point we may wish to supply admin endpoints for accessing alternate collection
      versions.
  * The user specifies the version, which to the collections API is an opaque string. Any
    semantics are supplied by the user.
    * Note that this means semantic versions will not sort in order - the DB and API would need
      specific schemas / code to do that.
* Activate a collection version.
  * Administration only operation.
  * Can activate any version at any time.
  * The active version is the only version accessible to users.
  * Separating the saving and activation steps means we can save a collection document,
    load data products for that collection, and then activate the collection when the data
    products are ready.
  * It also means we can trivially roll back changes.
* Get a collection version.
  * Public operation.
  * Initially, this is only the active collection.
    * At some point we may wish to supply admin endpoints for accessing alternate collection
      versions.
* List collections
  * Public operation
  * Only lists active collections.
* Longer term:
  * List all collections, even those without active versions.
    * Administration only operation.
    * For now, can be done relatively easily via the Aardvark Arango UI.
  * List collection versions
    * Administration only operation.
    * This may be a long list. Returning only the list of version strings for one collection 
      is probably reasonable up to ~100K versions, which seems much larger than we'll ever
      encounter.

## Collection contents

For a minimal start, more will be added later (e.g. data comparison methods, contributors, etc.)

* ID (lowercase alpha only string, no punctuation other than underscores, otherwise opaque)
* Name (opaque string)
* Version (opaque string)
* Source version (opaque string)
  * The source version may be the same from collection version to collection version if the
    same source data is reloaded to, say, fix bugs or add features.
  * This implies a particular collection version should not mix data from different source
    versions.
* Icon url
  * Could probably use FastAPI's built in static server to serve these for now
* Creation date
* (If active) activation date
* List of available data products with the active version of each data product (see example
  below)
  * Generally speaking, each data product will support a view in the collections UI
  * The version information allows creating a new version of the collection document but only
    updating some of the associated data products, while leaving others at their current
    version
  * Similar to the collections version, it also allows for easy rollbacks, and any version
    semantics are up to the user.
  * In the future, we could add a schema version if the schema needs to change. For now, no
    schema version implies a schema version of 1.
    * An altenative would just be to add a version to the product name, like `taxa_freq2`.

```
[
    {
        "product": "taxa_freq",
        "version": "r207.kbase.3"
    },
    {
        "product": "genome_stats",
        "version': "r207.kbase.14"
    }
]
```

## Implementation

### DB schema

* The collections document will be essentially as represented above, with the keys / values
  as described and a list of associated data products.
* There will be two separate collections - `coll_all_versions` (or something) and
  `coll_active_versions`.
  * The former holds all versions of all collections, and the latter holds only the active
    version of each collection.
* Activating a version means:
  * Pulling the collection version document from `coll_all_versions`
  * Setting the activation date to the current date
  * Overwriting the collection document in `coll_active_versions`.
    * Atomic!
* Alternatives:
  * Tag the active version in `coll_active_version`.
    * There's a fundamental problem with this approach in that it's non-atomic - at some point
      either no versions will be tagged as active or > 1 will.
    * This can be alleviated with transactions but in Arango [transactions are only reliable in
      non-sharded environments](https://github.com/arangodb/arangodb/issues/11424).
    * Furthermore, filtering data becomes much slower since all the inactive versions need to
      be filtered out. Index size goes up because you need (at least) the activity state in
      the equality part of the index. If you want to filter inactive versions, you need a
      separate index without the activity state in the equality part of the index.
    * Also generally more complex than the proposed schema.
  * Have the service manage the collection version with a monotonically increasing version, like
    `workspace_deluxe` or `sample_service`
    * This approach is much more complex and is not necessary for the use case proposed here. It
      also has the filtering drawback as mentioned above.

### Endpoints

* Only includes immediately relevant endpoints.

#### List collections

* For now returns all active collections based on the < 1000 collections assumption.

```
GET /collections
```

#### Save a collection

* Requires a token with the `COLLECTIONS_ADMIN` (or something) `auth2` role.

```
POST /collections/{collection_id}/versions/{version}

BODY contains collection contents.
```

#### Activate a collection

* Requires a token with the `COLLECTIONS_ADMIN` (or something) `auth2` role.

```
PUT /collections/{collection_id}/versions/{version}/activate
```

#### Get a collection

```
GET /collections/{collection_id}
```

* Extendable to allow getting a specific version in the future.

#### Data products

* General endpoint structure, exact path and query params will depend on the data product

```
GET /collections/{collection_id}/data/{data_product_id}/...
```
* Extendable to allow getting a specific collection version (which specifies the data product
  version) in the future.