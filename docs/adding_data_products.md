# Adding data products to the Collections service

This document describes how to add new data products to the collections service. It assumes
the reader is familiar with

* [FastAPI](https://fastapi.tiangolo.com/) and [Pydantic](https://pydantic-docs.helpmanual.io/)
* KBase infrastructure in general
* The Collections service
* Optionally, `docker-compose`

## Short version

* Look at the existing implementations for data products in
  [/src/service/data_products](/src/service/data_products):
  * [taxa_count](/src/service/data_products/taxa_count.py)
  * [genome_attributes](/src/service/data_products/genome_attributes.py)
* ... and follow their example.
* Add your new data product to the list in
  [/src/service/data_product_specs.py](/src/service/data_product_specs.py)

## Long version

* Choose an appropriate ID for your data product.
  * This ID will appear in the endpoint for your data product and will be included in the
    `data_products` list of a Collection document to indicate the data product is active for
    that Collection.
    * It's also advised to include the data product ID in any Arango database collections
      that support the data product.
  * Allowed characters are `a-z` and `_`.
* Create a new python module for your data product in
  [/src/service/data_products](/src/service/data_products) with a file name of
  `<data_product_id.py>`.
* Add any ArangoDB collection names and names for fields in the collection to
  [/src/common/storage/collection_and_field_names.py](/src/common/storage/collection_and_field_names.py)
  so that
  * they can be shared with loaders for your collection
  * there is a single document in the code base listing all collection names
  * we avoid collection name collisions.
* Be sure to read the comments in that file regarding how to construct your collection names
  and name their variables.
* Create a `FastAPI` `APIRouter` with a tag containing a short name for your data product in
  your data product module.
* Create a class inheriting from [DataProductSpec](/src/service/data_products/common_models.py)
  in your data product module, specifying your data product ID, router, and the ArangoDB
  collections and indexes your data product will use.
  * The class must implement the following methods with the given signatures
    ```python
    async def delete_match(self, storage: ArangoStorage, internal_match_id: str) -> None:
    ```
    * When called, this method must delete any match data associated with the given internal
      match ID.
    ```python
    async def apply_selection(self, storage: ArangoStorage, selection_id: str) -> None:
    ```
    * When called, this method must apply the given selection to the data. See the `genome_attribs`
      data product for an example.
* Create the endpoints / routes for your data product in the new module.
  * See the existing implementations for examples.
  * Routes must start with `/collection/{collection_id}/data_products/<data_product_id>/`
    * Everything up to the data product ID is handled by the app. The path starting from the ID is
      the responsibility of the data product developer.
  * There is already a Collection ID validator available in
    [/src/service/routes_common.py](/src/service/routes_common.py)
* To get application dependencies call the `src.service.app_state.get_app_state()`
  method, providing the `FastAPI` `Request` as the argument.
  * The storage `aql()` method will allow you to run arbitrary AQL queries against the database.
* Add your new data product to the list in
  [/src/service/data_product_specs.py](/src/service/data_product_specs.py)
* Test!
  * `docker-compose` might help here