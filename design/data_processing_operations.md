# Data operations processing design

## Document purpose

Lay out goals / designs for KBase Collections data processing in the short, middle and long term.
Said goals / designs will necessarily get more vague the further out in time we go.

## Short term goals

### Support collection set creation

Make the changes necessary to include object UPAs with data loads so the collections service
can create sets from data selections.

* Include Assembly UPAs: https://kbase-jira.atlassian.net/browse/RE2022-157
* Include Genome UPAs in the download metadata for the Assembly UPA.
  * We don't need the genome object for anything currently, we just need the UPA to allow the
    collections service to create sets.
  * Update the workspace downloader to run through the genomes in the workspace and record their
    Assembly references
    * Unfortunately this requires pulling the object as some genomes have > 1 reference
    * Subsetting the object might help
  * Save the genome UPA in the appropriate assembly metadata
  * Also save a metadata file in the UPA directory for the Genome so that it doesn't need to be
    pulled again

## Middle term goals

### Don't download non-WS files that are already downloaded

  * Primarily GTDB/NCBI for now. Should fix this before we download r214
  * Similar to `sourcedata/WS`, create a directory (`sourcedata/NCBI`) that holds all the data for
    a data source
  * For release versions, either
    * Create a new directory and link relevant folders in the global directory
    * Store release version information in a DB (see below)

### Don't recalculate results that are already available

  * For now assume that all results can be merged together for loading into arango and still be
    valid
    * E.g. there aren't any global calculations for the entire data that make subsets of the
      data invalid
  * Add a local (Mongo / MySQL / etc.) database to store file metadata and calculation information.
    * Mongo is probably the best choice just because we use it everywhere else and many people
      are familiar with it.
    * Store the Mongo files in the `root_dir` somewhere
      * This means Mongo will be running over NFS, which isn't ideal, but performance shouldn't
        be a huge issue for this application
      * Applications will need to make sure they don't start multiple copies of Mongo
      * Step towards the more automated long term system, which will probably require a DB
      * Alternative - host mongo somewhere else to allow devops to do backups etc
    * SQLite was considered, but there are warnings all over the documentation and email list
      re not using it over networked file systems such as the NERSC file system.
  * For each assembly, store the file location, calculation parameters (including the docker image
    hash) and calculation results (or the location of the calculation results, if they're not
    amenable to document based storage) for that assembly in the DB.
    * Assemblies for now, more data types in the future.
    * The file location should be stored as the source file location - e.g. in `sourcedata/WS` -
      as opposed to the collection specfic link.
      * Alternatively maybe the collection and source version data can be stored in the DB as well
        and we remove those directories.
    * The DB will take over the role of the metadata file written by the calculation scripts.
    * It could also theoretically take over the role of the source directory JSON metadata files.
  * The calculation script should update the DB as the results complete.
  * The parser script will simply collate the results from the DB and produce the JSONL upload
    files.
  * The job generation and / or calculation scripts should use the DB, input file, and calculation
    parameters to determine whether the calculation has already occurred for an input file.
    * This also allows for recovering from failed jobs or picking up from where a job left off.

### Tune the job generation script to minimize wasted node hours

See the following tickets:

  * https://kbase-jira.atlassian.net/browse/RE2022-150
  * https://kbase-jira.atlassian.net/browse/RE2022-96

### Upload external data (Assemblies and Genomes) to the workspace

  * Primarily GTDB for now
  * Add batch upload to `GenomFileUtil`
    * Which should create the assembly and genomes
      * Although the assembly ref isn't returned - should add that
    * Deal with any upload problems
      * Reported to be slow and unreliable, particularly when running > 2 containers at once
  * Add an upload script that is the equivalent of the download script, but starts with source
    data other than `WS` and
    * Creates the workspace objects
    * Adds the assembly file and meta data file into the appropriate `sourcedata/WS`
      subdirectories
    * Adds a collection name and source version directory and links the objects into that directory

### Move images and code in non-standard locations to GHA and / or main or develop branches

  * Computation tool images
  * Callback server image
  * Callback server branch in JobRunner repo

## Long term goals

NOTE: The data transfer service (DTS) effort has some similarities to this goal. We should
coordinate with that team to avoid duplicating efforts.

Ultimately, we want a mostly automated system that

* Detects changes in source data
  * In some cases it might make more sense for KBase personnel to trigger the load via some
    service endpoint or CLI
      * e.g. GTDB which releases rarely and has complications re loading, as the GTDB_tk app
        probably needs updating, collections which have old gtdb lineages in the
        `genome_attributes` data product need to be updated, etc.
      * Also when a tool is updated and we want to update the collection data
* Assigns an appropriate load version to the data load
* Downloads any necessary files
* Runs the tools on the new files in Slurm
* Runs the parser script on the new files
  * ... merging the new results with the pre-existing results and
  * producing files suitable for import to Arango
* Loads the files into Arango
* Updates the collection version in the service with the new load version for the data product
* Notifies KBase personnel who check the load, and if everying looks ok, activate the updated
  collection

