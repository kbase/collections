# PROTOTYPE - Collections repo

Contains service API and loader code for collections of data that

* users can compare with their data
* users can subselect
* users can move subselections into their own narratives

Currently collections only contain KBase staff curated data.

## File structure

TODO more details

* /service - service code
* /loaders/[collection ID] - loader code for collections, e.g. /loaders/gtdb

## Development

### Adding code

* In this alpha / prototype stage, we will be PRing (do not push directly) to `main`. In the
  future, once we want to deploy beyond CI, we will add a `develop` branch.
* The PR creator merges the PR and deletes branches.

### Code requirements:

* Any code committed must at least have a test file that imports it and runs a noop test so that
  the code is shown with no coverage in the coverage statistics. This will make it clear what
  code needs tests when we move beyond the prototype stage.
* Any code committed must have regular code and user documentation so that future devs
  converting the code to production can understand it.
* Release notes are not strictly necessary while deploying to CI, but a concrete version (e.g.
  no `-dev*` or `-prototype*` suffix) will be required outside of that environment. On a case by
  case basis, add release notes and bump the prototype version (e.g. 0.1.0-prototype3 ->
  0.1.0-prototype4) for changes that should be documented.

## TODO

* Set up devops build GHAs
* Set up test GHA
* Set up LGTM (look into the github integration - can we use that now and not have to convert?)
