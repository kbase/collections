# PROTOTYPE - Collections repo

Contains service API and loader code for collections of data that

* users can compare with their data
* users can subselect
* users can move subselections into their own narratives

Currently collections only contain KBase staff curated data.

## Usage

OpenAPI documentation is provided at the `/docs` endpoint of the server (in KBase, this is
at `<host>/service/collections/docs`, for example
[https://ci.kbase.us/services/collections/docs](https://ci.kbase.us/services/collections/docs)).

### Error codes

Error codes are listed in [errors.py](src/service/errors.py).

## Administration

To start the service Docker container:

* The collections listed in
  [collection_and_field_names.py](src/common/storage/collection_and_field_names.py) must be
  created in ArangoDB. The collections are not created automatically to allow service admins
  to specify sharding to their liking. Indexes are created automatically, assuming the collections
  exist.
* A script is provided for assistance in setting up the collections. For more info, run
```
$ PYTHONPATH=. python src/service_manager.py -h
```
* The environment variables listed in
  [collections_config.toml.jinja](collections_config.toml.jinja)
  must be provided to the Docker container, unless their default values are acceptable.
  In particular, database access and credential information must be provided.

## File structure

* `/src/service` - service code
* `/src/loaders/[collection ID]` - loader code for collections, e.g. `/loaders/gtdb`
* `/src/common` - shared loader and service code
* `/src/common/storage` - data connection and access methods
* `/test/src` - test code. Subdirectories should mirror the folder structure above, e.g.
  `/test/src/service` contains service test code

## Development

### Adding code

* In this alpha / prototype stage, we will be PRing (do not push directly) to `main`. In the
  future, once we want to deploy beyond CI, we will add a `develop` branch.
* The PR creator merges the PR and deletes branches (after builds / tests / linters complete).
* To add new data products, see [Adding data products](/docs/adding_data_products.md)

#### Timestamps

* Timestamps visible in the API must be fully qualified ISO8601 timestamps in the format
  `2023-01-29T21:41:48.867140+00:00`.
* Timestamps may be stored in the database as either the above format or as Unix epoch
  milliseconds, depending on the use case.
* If timestamps are stored as epoch ms, they must be converted to the ISO8601 format prior to
  returning them via the API.

### Versioning

* The code is versioned according to [Semantic Versioning](https://semver.org/).
* The version must be updated in
  * `/src/common/version.py`
  * `/RELEASE_NOTES.md`
  * any test files that test the version

### Code requirements for prototype code:

* Any code committed must at least have a test file that imports it and runs a noop test so that
  the code is shown with no coverage in the coverage statistics. This will make it clear what
  code needs tests when we move beyond the prototype stage.
* Each module should have its own test file. Eventually these will be expanded into unit tests
  (or integration tests in the case of app.py)
* Any code committed must have regular code and user documentation so that future devs
  converting the code to production can understand it.
* Release notes are not strictly necessary while deploying to CI, but a concrete version (e.g.
  no `-dev*` or `-prototype*` suffix) will be required outside of that environment. On a case by
  case basis, add release notes and bump the prototype version (e.g. 0.1.0-prototype3 ->
  0.1.0-prototype4) for changes that should be documented.

### Running tests

Python 3.11 must be installed on the system.

```
pipenv sync --dev  # only the first time or when Pipfile.lock changes
pipenv shell
PYTHONPATH=. pytest test
```

## TODO

* Logging ip properly (X-RealIP, X-Forwarded-For)
  * Add request ID to logs and return in errors
  * Compare log entries to SDK and see what we should keep
    * Take a look at the jgi-kbase IDmapper service

### Prior to declaring this a non-prototype

* Coverage badge in Readme
* Run through all code, refactor to production quality
* Add tests where missing (which is a lot) and inspect current tests for completeness and quality
  * E.g. don't assume existing tests are any good
  * Async testing help
    https://tonybaloney.github.io/posts/async-test-patterns-for-pytest-and-unittest.html
* Build & push tool images in GHA
  * Consider using a base image for each tool with a "real" image that builds from the base image.
    The "real" image should just copy the files into the image and set the entry point. This will
    make GHA builds a lot faster
  * Alternatively use docker's GHA cache feature
  * Manual push only is probably fine, these images won't change that often
* JobRunner repo should be updated to push the callback server to a GHA KBase namespace
* Testing tool containers
  * DO NOT import the tool specific scripts and / or run them directly in tests, as that will
    require all their dependencies to be installed, creating dependency hell.
  * Instead
    * Test as a black box using `docker run`
      * This won't work for gtdb_tk, probably. Automated testing for that is going to be
        problematic.
    * If necessary, add a `Dockerfile.test` dockerfile to build a test specific image and run
      tests in there.
      * Either mount a directory in which to save the coverage info or `docker cp` it when the
        run is complete
      * Figure out how to merge the various coverage files.
