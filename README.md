# PROTOTYPE - Collections repo

Contains service API and loader code for collections of data that

* users can compare with their data
* users can subselect
* users can move subselections into their own narratives

Currently collections only contain KBase staff curated data.

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
* Any code committed must have regular code and user documentation so that future devs
  converting the code to production can understand it.
* Release notes are not strictly necessary while deploying to CI, but a concrete version (e.g.
  no `-dev*` or `-prototype*` suffix) will be required outside of that environment. On a case by
  case basis, add release notes and bump the prototype version (e.g. 0.1.0-prototype3 ->
  0.1.0-prototype4) for changes that should be documented.

### Running tests

Python 3.10 must be installed on the system.

```
pipenv sync --dev  # only the first time or when Pipfile.lock changes
pipenv shell
PYTHONPATH=. pytest test
```

## TODO

* templating - jinja / envsubst / ?
* Logging ip properly (X-RealIP, X-Forwarded-For)
* Error handling and representation in the API

### Prior to declaring this a non-prototype

* Coverage badge in Readme
* Run through all code, refactor to production quality
* Add tests where missing (which is a lot)
  * Async testing help https://tonybaloney.github.io/posts/async-test-patterns-for-pytest-and-unittest.html