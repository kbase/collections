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
        * cannot use index without ArangoSearch View or (maybe?) inverted index
          * TBD - will an inverted index work?
        * Out of scope for v1
  * Enum
    * Only implement if needed
  * Hidden
    * Not visible to users
      * Primarily for hiding the full classification string if we split it into parts
* Indexing
  * Don't add any indexes right off the bat
  * If needed, collect data on popular filters and add indexes
    * For really popular & slow filter combinations, add compound indexes
  * See notes on string types above
* Filter operations
  * Add AND / OR toggle for combining filters
  * Move selection / match mark booleans into input filter data structure
  * We may want to split the classification field into multiple fields
    * Alternatively make a custom filter for the classification field that understand
      the string syntax
      * Would mean a new column type like `taxon`
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