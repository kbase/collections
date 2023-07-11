# Paging

## Document purpose

Describe options for paging through collections data (although this discussion is generally
applicable to any data stored in a database that is too large to return to the user at once)
and their pros and cons.

Paging is often an afterthought or considered a non-issue, but this is not the case, and there
is no ideal solution that the author is aware of. Database schemas and APIs may need modification
to page effectively and efficiently if not designed with that in mind.

## Nomenclature

* `documents` can be read as documents in in a document based database, like MongoDB or ArangoDB,
  or rows in a RDBMS, like MySQL or Postgres.

## Options

### Skip and limit / offset and limit

This is the standard paging solution of providing a count of documents to skip and a count
of documents to return.

#### Pros

* Can page to any location in the data
* Easily understood by users

#### Cons

* `O(n^2)` time complexity for most database implementations
  * This means that in larger data sets deep paging gets progressively, exponentially slower
  * A unlimited skip and limit implementation in the KBase Workspace caused MongoDB to slow to
    a literal halt due to UI paging code written by someone who should've known better
    * Skip / limit was removed after that
  * Often implemented with a maximum skip allowed to prevent slow paging
    * Elasticsearch's limit is 10K by default
    * This means that only the data at the front of the sort can be paged

### `start_after` with limit

Provide a string or numerical value after which paging should start. Get the next start after
value from the last value of the current page.

E.g. if the current page runs from `Ababdeh` to `abaciscus` the `start_after` value for the next
page would be `abasciscus`.

#### Pros

* Easily understood by users if implemented as a simple "next page" button
* Users can jump to a particular value rather than having to search through pages if they
  are allowed to specify the `start_after` value
  * Essentially a filter used for paging
* Prior `start_after` values can be cached by the front end to allow for returning to a
  previous page
* At worst linear time complexity

#### Cons

* Can only navigate to the next page after the current page. Navigating to page 10 from page
  1 would require 9 full page loads.
* May not work well on non-unique fields
  * If a page has more than a page size's number of identical values in the field being paged over,
    it is impossible to advance further.
  * A numbered index could be added for each field which might be subject to sorting and those
    numbers could be used as the `start_after` value.
    * This adds otherwise valueless data to the database data and index size
    * Similar to the `Precalculated or inherent ordering` strategy below but unaffected by
      filtering and cannot skip pages

#### Notes

* This is one of the paging strategies the KBase Workspace supports, based on the object Unique
  Permanent Address

### Precalculated or inherent ordering

In this case, an ordering is applied to every field that may be paged over prior to loading the
data to a database, or the field has an inherent ordering (like an autoincrementing ID).
The pages can then be retrieved by providing an order range.

#### Pros

* Can page to any location in the data
* Easily understood by users
* At worst linear time complexity

#### Cons

* Any data filtering or deletion disrupts the ordering and may result in pages less than the
  expected size or entirely empty pages

#### Notes

* This is one of the paging strategies the KBase workspace supports, using the object ID within
  a particular workspace.

### Calculate and cache paging

In this case, when a user submits a query (possibly with filtering) to the API, the service,
as well as returning the first page, calculates the `start_after` value for every other page
(or blocks of pages to save space) in the query. This index of sorts is then saved back to the
database (to allow for horizontally scaling the API) with a query ID, and the ID is returned
to the user for use when requesting further pages.

#### Pros

* Can page to any location in the data
* Easily understood by users
* Works with data filtering
* Can be mixed with standard skip and limit to only calculate the index once the user exceeds
  some skip value threshold
* At worst linear time complexity

#### Cons

* By far the most complex to implement
* Has to process the entire query up front, as opposed to the first `limit` entries, to
  generate the index
  * If the dataset is very large this could take a very long (linear amount of) time
  * Potentially the index could be generated in the background and the available pages at
    any given time communicated to the front end, but that adds even more complexity
* The user must retrieve and use the query ID to take advantage of the generated index
