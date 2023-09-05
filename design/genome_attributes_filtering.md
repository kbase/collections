# Genome Attributes Filtering design v1

* Add a meta endpoint that returns
  * The column names
  * The type for each column
  * The filter strategy for types that have more than one
  * Min and max range for numbers and dates
  * Enum list for enums
* Column types
  * number - range filter
    * Do we need to distinguish between int and float? We do for microtrait
    * Allow specifying inclusivity at each end of the range
  * date - range filter, ISO8601 input
      * Allow specifying inclusivity at each end of the range
  * string
    * Maximum string length
      * No need for this currently AFAWK
    * Filter strategies
      * prefix search
      * full text search
        * requires ArangoSearch View or inverted index
      * substring search
        * cannot use index without ArangoSearch View or inverted index
        * For `LIKE` [based matching](https://www.arangodb.com/docs/stable/aql/functions-arangosearch.html#like)
          effectively performs an index scan
        * n-gram search may be an option if `LIKE` based search is too slow
        * Out of scope for v1
  * Enum
    * Only implement if needed
  * Hidden
    * Not visible to users
      * Primarily for hiding the full classification string if we split it into parts
* Indexing
  * ~Don't add any indexes right off the bat~
    * Will need ArangoSearch views for full text search
    * Makes sense to add all the columns to the view so that other text searches
      and filters can be executed as part of the search
      * Otherwise filters are applied to the search results, which may be large
  * ~If needed, collect data on popular filters and add indexes~
    * ~For really popular & slow filter combinations, add compound indexes~
  * See notes on string types above
  * On Thursday August 31 the RE team
    [agreed](https://kbase.slack.com/archives/C03FK9RKSBX/p1693516341250389)
    that we wouldn't build and specialized tooling for updating indexes / views and would
    handle updates on a case by case basis
    * E.g. if a new field was added to the genome attributes collection that needed
      a `text_en` analyzer vs. the standard identity analyzer
    * All index / view updates will be manual
    * This likely means either
      * Deleting the current view and recreating it, during which time search queries
        will fail or return no results, presumably
      * Creating a new view and pointing the server at it (which might need some
        additional code to allow configuring the view name), then deleting the old view
* Filter operations
  * Add AND / OR toggle for combining filters
  * ~Move selection / match mark booleans into input filter data structure~
    * Decided to implement filters as individual query parameters, so this no longer
      makes sense
  * We may want to split the classification field into multiple fields
    * Alternatively make a custom filter for the classification field that understands
      the string syntax
      * Would mean a new column type like `taxon`
    * Note that ArangoSearch views will tokenize the field
* Out of scope for v1
  * Geo search
  * Substring search
  * Multiselect on non-enum columns
  
TODO:  
Paramvir - for string columns, what type of search do we want  
Paramvir - do we want to split up the taxonomy field?  
Gavin - design filter API, approve w/ David & Tian  
Gavin - update service to support filtering  
Tian - add column config file to pipeline & parse columns data into metadata document  
David - UI  