# Sample flattening design doc

## Nomenclature

GA - genome attributes

## Background

To speed implementation, when the sample table was first implemented in Collections it was
created such that for every row in the GA table there was a corresponding row in the samples
table. That meant that there were many duplicate rows in the samples table since one sample can
be associated with many genomes.

The intent of this design document is to provide a plan for how samples would be reimplemented
so the sample table would only contain a single row per sample, but selection would still
work between the genome and sample tables.

## Design

* The loader would be reworked so that only one row per sample would be created.
  * The sample ID for each genome would be added to the GA table entries.
    * This could be exposed to the user and included in filters.
    * The sample page could link back to the genome page on click with a filter
      already set up for that sample ID.
  * Either (or both) of:
    * A list of associated genome IDs would be added to each row in the sample table
    * Edges from samples to associated genomes would be added.
    * Edges are more "RE-like" but may have performance impacts since many more
      documents will have to be read in order to collate the list of associated
      genomes.
      * We will try both and measure the performance of each then make a decision.
  * The count of genomes associated with a sample will be added to
    each sample row.
    * This will be visible to users.
* Filters will not work cross-table, at least for the first iteration.
  * Some of the team felt it would be confusing for users to have filters on another page
    affect the page they're looking at.
  * It would be complex to implement.
* Selections will work cross-table and can be used in a way similar to how cross-table filtering
  might work.
  * For example:
    * Filter on table A
    * Create a selection based on the filter
      * This might require an endpoint to create a selection from a filter as
        presumably the UI can't do this without pulling all the data that matches
        the filter, which could be substantial
        * Alternatively the user can only select what's on the page for the first
          iteration
        * The selection limit of 10K comes into play here
    * Switch to table B, activate the selection, and filter
      * Create a new selection based on the filter
    * Repeat from step 1
  * This will require at least one of two new sample endpoints:
    * An endpoint to get genome IDs associated with a list of samples
      * The UI would then create a selection with this list
    * An endpoint to create a selection based on a list of samples or a filter set
      as above.
    * In either case, could wind up with a large list of genomes, 10K selection limit
      comes into play again
  * The main samples endpoint would also need to be updated to to get selection information
    so the UI can display it to the user
    * For each genome in the selection, look up the associated sample. Sum up the
      genomes in the selection and return a list of samples with the count of
      selected genomes and the total associated genomes per sample.
      * This will allow showing whether a sample has no, some, or all associated
        genomes selected
  * The drawback to this approach is that the exact provenance for how a selection is created is,
    at best, difficult to record vs. recording a single multi-table wide filter expression.
    * Even then we allow users to add and remove items from the selection at will
      so provenance is still difficult to record
    * We will consider provenance at a later date
    * Also, since collections are ephemeral, that means provenance eventually becomes
      useless as the source data is no longer available.
    * However, the idea is that in the future collections will be built on data that
      is not ephemeral so determining how to execute provenance capture may be a
      useful thing.
