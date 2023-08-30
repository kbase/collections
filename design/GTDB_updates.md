# GTDB update strategy

How to handle GTDB updates was discussed at the RE team weekly meeting on 2023/8/29.

Attendees:  
* Paramvir Dehal
* Meghan Drake
* Gavin Price
* Dakota Blair
* Cody O'Donnel
* Sijie Xiang

## Resources

[GTDB_tk KBase app](https://github.com/kbaseapps/kb_gtdbtk)

## Background

GTDB updates their database approximately once per year. This impacts KBase Collections in
several ways:

* The KBase app needs to be updated to the new version.
  * If the app is updated to only support the new version, the GTDB lineage matcher will no
    longer work with any genomes / assemblies annotated by that app.
  * For the v207 -> v214 update, the app maintainer plans to make the version selectable at
    runtime so that users that wish to use the lineage matcher can annotate their data with
    the earlier version. 
    * However, data annotated with v214 will not work with the matcher until the
      collections are updated with v214 and the corresponding matcher reparameterized.
    * The UI will need clear errors / instructions for the user so they can take
      the appropriate steps to annotate their data in a compatible way.
* All the collections' genome attributes data need to be recreated with the new version of the
  GTDB_tk binary and their GTDB lineage matcher reparameterized.
  * If this update is done prior to the GTDB_tk app being updated, the lineage matcher will not
    work with any genomes / assemblies in KBase.
  * Data annotated by the earlier version of the app will not work with the lineage matcher
    until said data is reannotated.
* If using the GTDB metadata file to populate genome attributes data rather than running the
  Collections tool pipeline, the new version of the metadata file will need to be run through
  the pipeline.
  * Similarly to other collections, data annotated with the prior version of the GTDB_tk app
    will not work with the lineage matcher.

## Options

2 options were discussed by the team.

### Maintain multiple GTDB versions in each collection

* The tool pipeline would be modified to include GTDB classifications for multiple versions by
  either
  * Running multiple versions of the GTDB_tk binary
  * Making use of update maps provided by GTDB
* The matcher would be updated to allow for parameterizing multiple GTDB versions rather than
  just one
* This would allow the matcher to select the appropriate classification field to match against
  given the GTDB version used to annotate the genomes / assemblies
  * Mixing versions in a single match would not be allowed
* As such, the collections could be upgraded to a new version of GTDB prior to the app being
  upgraded and all annotated data would continue to work with the lineage matcher. The app could
  then be upgraded and both new and old annotated data would work with the matcher.
* In other words, a lockstep upgrade to the app and collections is not required to continue
  supporting all GTDB annotated data in Collections.

### Maintain a single version

* In this case we would not make substantial changes to the tool pipeline or the matchers other
  than switching to the new GDTB_tk binary version.
* We may need minor UI changes to give users appropriate feedback about what their options are
  if their data is not annotated in such a way that it is compatible with the lineage matchers.
* If the app is updated before collections, any data annotated with the new version of the app
  (and the new GTDB version if the app allows version selection) will not work with the lineage
  matcher.
* If collections are updated before the app, then no KBase annotated data will be compatible with
  the lineage matcher.
* The users would have workarounds:
  * The homology matcher
  * If the app supports multiple GTDB versions and the app is updated before the collections,
    they could annotate the data with the older version of GTDB.
  * The UI would need to make these workarounds clear if a version conflict occurs.

## Decision

The team decided to move forward with option 2.

## Next steps

* Wait for release of the GTDB_tk KBase app that supports both v207 and v214.
* Update the tool pipeline to GTDB v214.
* Run the genome attribs tool pipeline for all the collections and update the collection data and
  lineage matcher in the Collections service.
  * If GTDB is still using the metadata files as the source data, rerun the pipeline with the
    new source files and update the collection data and lineage matcher.
* Update the UI to display workarounds when a version conflict error is returned from the server.
 