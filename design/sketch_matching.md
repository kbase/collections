# Homology matcher design

## Document purpose

* Propose a design for a homology / sketch matcher for the KBase Collections project.
* Review current infrastructure and potential reusability.

## Requirements

* Add a matcher to the KBase collections service that uses Minhash sketching to match to data
  in the collection.
* Support the sensitivity cutoff and coverage parameters in the matcher UI
  * Standard Mash only supports distance
  * CMash supports coverage
* We'll start with supporting Mash for now since the existing infrastructure already suports it
* Devs ideally don't want to have to use SDK App refdata for minhash sketches since that means
  going through an app release every time the sketches are updated

## Nomenclature

UPA - The Unique Permanent Address of a KBase workspace object

## Existing infrastructure

### kb_mash SDK app

Repo: https://github.com/kbaseapps/kb_mash

* Probably not useful for our purposes, but listed here for completeness.
* Doesn't accept the minimum distance parameter.

### kb_cmash SDK app

Repo: https://github.com/kbaseapps/kb_cmash

* Probably not useful for our purposes, but listed here for completeness.
* Doesn't accept Minhash parameters like cutoff, coverage, etc.

### Homology service

Repo: https://github.com/jgi-kbase/AssemblyHomologyService  
Host: https://homology.kbase.us/

* Takes a **single** Minhash sketch and queries one of a set of pre loaded databases
* Loading data is done via running a CLI with DB access and specifying a sketch
  database file that is accessible by the server
* Returns at most 1000 matches
* Does not accept any Minhash parameters like cutoff, coverage, etc.
* Only supports Mash sketches (https://github.com/marbl/Mash/)
* Only uses a single CPU for sketching, since by default that's what Mash does
* Potential improvements:
  * Support Minhash parameters
  * Update Mash to 2.3
  * Tell Mash to use more CPUs (test for diminishing returns)
    * Server admin parameter to make testing easy?
  * Take a input sketch database and match it against the pre loaded database rather than a single
    sketch
  * Test returning more than 1000 matches
  * API for loading databases
    * Pull databases into docker container on startup or creating / updating a namespace
      rather than requiring a preexisting file in a mount
  * OpenAPI docs
  * The sketch service has a retry loop for the Homology service, unlike other services,
    suggesting there are reliabilty issues. Stress test the service to confirm / deny.
  * Service has had no maintenance since April 2019 and is severely behind on dependencies,
    image build strategy and storage, etc. It's likely the build won't work.
  * All sketch service (see below) instances in all environments contact the same host,
    which makes it difficult to integration test changes. We may want to get devops help to
    install a standalone instance in CI and point the CI Sketch service at it.

### ID mapping service

Repo: https://github.com/jgi-kbase/IDMappingService  
Host: https://ci.kbase.us/services/idmapper/ (and other environments)

* Maps IDs from one namespace to another.
* Used by the sketch service (see below) to translate IDs for one hard coded Homology service
  namespace
  * Not necessary for our purposes (see below)
* Potential improvements:
  * Service has had no maintenance since November 2019 and is severely behind on dependencies,
    image build strategy and storage, etc. It's likely the build won't work.

### Cache service

Repo: https://github.com/kbase/file_cache_server  
Host: https://ci.kbase.us/services/cache/ (and other environments)

* Caches data for 30 days to prevent long recalculations.
* No bulk endpoints
* Potential improvements:
  * Add bulk cache creation, upload, and retrieval endpoints
    * Otherwise bulk caches will be mostly useless as a change in one input UPA means a cache miss
  * Service has had no maintenance since Feb 2021 and is severely behind on dependencies,
    image build strategy and storage, etc. It's likely the build won't work.
  * Make the service cache tokens: https://github.com/kbase/file_cache_server/issues/22

### Sketch service

Repo: https://github.com/kbaseapps/sketch_service  
Host: KBase dynamic service, look up in the KBase Catalog

* Takes a **single** UPA of a Workspace reads, assembly, or genome object and
  * Checks the Cache service to see if the result has been cached. If so, returns the result.
  * Creates a sketch from the reads or assembly data
  * Sends the sketch to the homology service
  * If the homology service namespace for the sketch database is `NCBI_Refseq`, translates
    the IDs returned from the homology service via the ID service
    * If we put our own `kbase_id`s into the sketch database for the Homology service, we don't
      need ID translation
  * Caches the results in the Cache service
  * Returns the results
* Does not accept any Minhash parameters like cutoff, coverage, etc.
* Only supports Mash sketches (https://github.com/marbl/Mash/)
* Only uses a single CPU for sketching, since by default that's what Mash does
* It's a KBase dynamic service but is **not** an SDK service. There's just enough manually created
  SDK output to allow it to register, but there's no spec file, etc.
* Potential improvements:
  * Still only in `beta` in all environments, needs to be promoted to release given it's
    being used to serve prod data
  * May want to consider making it a core service
  * Support Minhash parameters
  * Update Mash to 2.3
  * Tell Mash to use more CPUs (test for diminishing returns)
    * Server admin parameter to make testing easy?
  * Support input of many objects
    * Could be a lot of work, all the code assumes a single object, including the workspace
      client from https://github.com/kbaseIncubator/workspace_client_py
  * Service has had no maintenance since Feb 2021 and is severely behind on dependencies,
    image build strategy and storage, etc. It's likely the build won't work.

## Concerns

* Currently a match can support up to 10K input UPAs. Sketching and running that many inputs
  could take a very long time, likely longer than service timeouts.
  * Overwhelming services is also a concern, especially if we don't implement batching
  * Service drive space may also be an issue, esp for the sketch service

## Implementation options

1. Use the entirety of the existing infrastructure (Sketch, Cache, and Homology services)
  1. Might have scalability problems
  2. Updates to services to batch results might help
2. Skip the Sketch service, download and sketch in an app, and send the results to the Homology
   service
  1. Might have scalabilty problems, although less likely than using the sketch service since
     data downloads are skipped
  2. Updates to service to batch results might help
3. Use an SDK app, but store Collection sketches in a (ftp?) server, download to the app and do
   sequence download, sketching, and distance measurement in the app
  1. Note sketches are large, the ~100K sequence Refseq sketch currently in the Homology service
     is 7.7G
  2. Note that jobs are user specific and matches are not, implying we'll need to use a service
     token to generate agent tokens per job and submit jobs on behalf of the service
4. Use an SDK app with refdata for Collections sketches
  1. This means we have to release the app every time we change a collection or improve our
     sketches, which is a big time suck for the devs

## Design

### Service

* Assuming we reuse the infrastructure above (potentially with some changes to support
  batching, Minhash parameters, etc.) the design is very simple
  * Most of the work is probably in bringing the old services up to a reasonable state and
    upgrading them with the features we want / need
* Add a matcher taking the parameters we want to support
  * Name: `minhash`
* The matcher kicks off a process as usual
* The process sends the data to the sketch service (possibly in batches for large inputs,
  or even single object requests) and waits
  * Or starts an EE2 job and waits
* The process gets the matching `kbase_id`s back from the service and completes the match
  as usual

### Data pipeline

* Add the Mash tool (https://github.com/marbl/Mash/) to the calculation script.
  * The SDK app has install information, if needed
  * Experiment with batch sizes and concatenating the database in the parser script. IIRC,
    concatenation was somewhat expensive and may not be appropriate to run on the Permutter
    login node. OTOH maybe I'm wrong, it's been a while.
    * Per Shane this is fast, which if so, means we could potentially sketch each genome
      individually and assemble them all in the parser script.
  * Note that the final product will get loaded into a Homology service namespace, not ArangoDB.
    * The namespace is probably something like `<collection ID>_<load_version>`

## Decision

23/05/09: Stress test the sketch and homology services (without ID mapping) and go from there.
See how many distance measurements can be done in parallel without breaking things. Also test
parallelism with `mash`, which would be a really easy change to make in the Homology service.
