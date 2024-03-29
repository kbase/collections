# Experiments with ArangoSearch View creation

Intention is to get some ideas re how long it takes to create a view for all of genome attributes
(currently contains GTDB v207, PMI, GROW, and ENIGMA data with one load version each),
search it, etc.

There are currently 321,020 documents in the `kbcoll_genome_attribs` collection.

As far as the author can tell, views can only be modified by removing a collection from the view
and re-adding it, or by making a new view with the new desired fields and switching to it.

## Notes

All experiments were run on CI ArangoDB over an SSH tunnel from the author's laptop.

## Check for overlapping keys

The GTDB genome attribs data is currently sourced from the metadata files provided by GTDB whereas
other collections data come from checkm2 and gtdb_tk output. If the collections have overlapping
keys with different types or semantics that could cause issues for ArangoSearch.

Example documents from the GTDB and PMI collections were saved to local files for examination.

```python
In [9]: import json

In [10]: with open("./arangoimport_data_files/GTDB_example.json") as f:
    ...:     gtdb = json.load(f)
    ...: 

In [11]: with open("./arangoimport_data_files/PMI_example.json") as f:
    ...:     pmi = json.load(f)
    ...: 

In [12]: gtdb.keys() & pmi.keys()
Out[12]: {'_mtchsel', 'classification', 'coll', 'kbase_id', 'load_ver'}
```

The 5 overlapping fields are all intentionally shared fields with identical types and semantics.

## Create a new view

Create a new view indexing all the genome attributes properties and adding full text indices
for selected properties.

View size is 205MB vs. 151MB collection size.

```python
In [1]: with open("/home/crushingismybusiness/.arangopwdCIcollections_dev") as f
   ...: :
   ...:     arango_coll_dev_pwd = f.read().strip()
   ...: 

In [2]: import time

In [3]: import aioarango

In [4]: cli = aioarango.ArangoClient(hosts='http://localhost:48000')

In [5]: db = await cli.db("collections_dev", username="collections_dev", passwor
   ...: d=arango_coll_dev_pwd)

In [6]: analyzer = {"analyzers": ["text_en"]}

In [7]: t1 = time.time(); await db.create_arangosearch_view(
   ...:     name="genome_attribs_test",
   ...:     properties={
   ...:         "links": {
   ...:             "kbcoll_genome_attribs": {
   ...:                 "includeAllFields": True,
   ...:                 "fields": {
   ...:                      "classification": analyzer,
   ...:                      "classification_method": analyzer,
   ...:                      "note": analyzer,
   ...:                      "ncbi_assembly_name": analyzer,
   ...:                      "ncbi_genome_category": analyzer,
   ...:                      "ncbi_organism_name": analyzer,
   ...:                      "ncbi_submitter": analyzer,
   ...:                  }
   ...:              }
   ...:          }
   ...:      }
   ...:  ); t2 = time.time(); t2 - t1
Out[7]: 35.111151695251465

In [8]: await db.view("genome_attribs_test")
Out[8]: 
{'global_id': 'c198809776/',
 'id': '198809776',
 'name': 'genome_attribs_test',
 'type': 'arangosearch',
 'cleanup_interval_step': 2,
 'commit_interval_msec': 1000,
 'consolidation_interval_msec': 1000,
 'consolidation_policy': {'type': 'tier',
  'segments_min': 1,
  'segments_max': 10,
  'segments_bytes_max': 5368709120,
  'segments_bytes_floor': 2097152,
  'min_score': 0},
 'primary_sort': [],
 'primary_sort_compression': 'lz4',
 'stored_values': [],
 'writebuffer_idle': 64,
 'writebuffer_active': 0,
 'writebuffer_max_size': 33554432,
 'links': {'kbcoll_genome_attribs': {'analyzers': ['identity'],
   'fields': {'note': {'analyzers': ['text_en']},
    'ncbi_submitter': {'analyzers': ['text_en']},
    'ncbi_organism_name': {'analyzers': ['text_en']},
    'ncbi_genome_category': {'analyzers': ['text_en']},
    'ncbi_assembly_name': {'analyzers': ['text_en']},
    'classification_method': {'analyzers': ['text_en']},
    'classification': {'analyzers': ['text_en']}},
   'include_all_fields': True,
   'track_list_positions': False,
   'store_values': 'none'}}}

In [9]: t1 = time.time(); await db.delete_view(name="genome_attribs_test"); time
   ...: .time() - t1
Out[9]: 0.12676692008972168
```

## Test full text search

Via Aardvark

```
Query String (132 chars, cacheable: false):
  FOR d IN genome_attribs_test
      SEARCH ANALYZER(TOKENS("f__UBA183", "text_en") ALL == d.classification , "text_en")
      RETURN d

Execution plan:
 Id   NodeType            Site  Calls   Items   Runtime [s]   Comment
  1   SingletonNode       DBS       2       2       0.00002   * ROOT
  2   EnumerateViewNode   DBS       2      17       0.00377     - FOR d IN genome_attribs_test SEARCH ANALYZER(([ "f__uba183" ] all == d.`classification`), "text_en")   /* view query */
  6   RemoteNode          COOR      4      17       0.00515       - REMOTE
  7   GatherNode          COOR      2      17       0.00423       - GATHER   /* parallel, unsorted */
  3   ReturnNode          COOR      2      17       0.00001       - RETURN d

Indexes used:
 none

Optimization rules applied:
 Id   RuleName
  1   handle-arangosearch-views
  2   scatter-in-cluster
  3   remove-unnecessary-remote-scatter
  4   parallelize-gather

Query Statistics:
 Writes Exec   Writes Ign   Scan Full   Scan Index   Filtered   Peak Mem [b]   Exec Time [s]
           0            0           0           17          0          65536         0.01118

Query Profile:
 Query Stage           Duration [s]
 initializing               0.00000
 parsing                    0.00017
 optimizing ast             0.00012
 loading collections        0.00002
 instantiating plan         0.00003
 optimizing plan            0.00474
 executing                  0.00477
 finalizing                 0.00135
```

17 results

## Test substring search with `LIKE()`

Via Aardvark.

`LIKE()` [must do an index scan](https://github.com/arangodb/arangodb/issues/19712) unless
it's a prefix search (e.g. no leading `%` or `_`).

```
Query String (146 chars, cacheable: false):
 FOR d IN genome_attribs_test
     SEARCH ANALYZER(LIKE(d.classification, CONCAT("%", TOKENS("UBA18", "text_en")[0], "%")), 
 "text_en")
     RETURN d
 

Execution plan:
 Id   NodeType            Site  Calls   Items   Runtime [s]   Comment
  1   SingletonNode       DBS       2       2       0.00002   * ROOT
  2   EnumerateViewNode   DBS       2     209       0.01055     - FOR d IN genome_attribs_test SEARCH ANALYZER(LIKE(d.`classification`, CONCAT("%", [ "uba18" ][0], "%")), "text_en")   /* view query */
  6   RemoteNode          COOR      6     209       0.01909       - REMOTE
  7   GatherNode          COOR      3     209       0.01115       - GATHER   /* parallel, unsorted */
  3   ReturnNode          COOR      3     209       0.00001       - RETURN d

Indexes used:
 none

Optimization rules applied:
 Id   RuleName
  1   handle-arangosearch-views
  2   scatter-in-cluster
  3   remove-unnecessary-remote-scatter
  4   parallelize-gather

Query Statistics:
 Writes Exec   Writes Ign   Scan Full   Scan Index   Filtered   Peak Mem [b]   Exec Time [s]
           0            0           0          209          0         688128         0.02395

Query Profile:
 Query Stage           Duration [s]
 initializing               0.00000
 parsing                    0.00013
 optimizing ast             0.00010
 loading collections        0.00001
 instantiating plan         0.00003
 optimizing plan            0.00394
 executing                  0.01856
 finalizing                 0.00121
```

209 results

Seems as though if a substring search is submitted with whitespace we'd need to split the string
server side and `AND` multiple `LIKE` clauses together, but this could easily cause bugs if
we don't match the arango-side tokenizer or if it changes.

Also see https://www.arangodb.com/docs/3.9/arangosearch-wildcard-search.html for how to
massage user input to deal with characters with special meanings.

## Test substring search with n-grams

Set up the analyzer and view programatically.

View size is 274MB vs. 151MB collection size.

```python
In [1]: with open("/home/crushingismybusiness/.arangopwdCIcollections_dev") as f
   ...: :
   ...:     arango_pwd = f.read().strip()
   ...: 

In [2]: import time

In [3]: import aioarango

In [4]: import requests

In [5]: ret = requests.post(
   ...:     "http://localhost:48000/_db/collections_dev/_api/analyzer",
   ...:     auth=("collections_dev", arango_pwd),
   ...:     json={
   ...:         "name": "trigramUTF8lower",
   ...:         "type": "pipeline",
   ...:         "features": ["position", "frequency"],
   ...:         "properties": {"pipeline": [
   ...:             {
   ...:                 "type": "norm",
   ...:                 "properties": {
   ...:                     "locale": "en",
   ...:                     "case": "lower",
   ...:                     "accent": False
   ...:                 }
   ...:             },
   ...:             {
   ...:                 "type": "ngram",
   ...:                     "properties": {
   ...:                     "min": 3,
   ...:                     "max": 3,
   ...:                     "preserveOriginal": False,
   ...:                     "streamType": "utf8"
   ...:                  }
   ...:             }
   ...:         ]}
   ...:     }
   ...: )

In [6]: ret.json()
Out[6]: 
{'name': 'collections_dev::trigramUTF8lower',
 'type': 'pipeline',
 'properties': {'pipeline': [{'type': 'norm',
    'properties': {'locale': 'en', 'case': 'lower', 'accent': False}},
   {'type': 'ngram',
    'properties': {'min': 3,
     'max': 3,
     'preserveOriginal': False,
     'streamType': 'utf8',
     'startMarker': '',
     'endMarker': ''}}]},
 'features': ['frequency', 'position']}

In [7]: cli = aioarango.ArangoClient(hosts='http://localhost:48000')

In [8]: db = await cli.db("collections_dev", username="collections_dev", passwor
   ...: d=arango_pwd)

In [9]: analyzers = {"analyzers": ["text_en", "trigramUTF8lower"]}

In [10]: t1 = time.time(); await db.create_arangosearch_view(
    ...:     name="genome_attribs_ngram_test",
    ...:     properties={
    ...:         "links": {
    ...:             "kbcoll_genome_attribs": {
    ...:                 "includeAllFields": True,
    ...:                 "fields": {
    ...:                      "classification": analyzers,
    ...:                      "classification_method": analyzers,
    ...:                      "note": analyzers,
    ...:                      "ncbi_assembly_name": analyzers,
    ...:                      "ncbi_genome_category": analyzers,
    ...:                      "ncbi_organism_name": analyzers,
    ...:                      "ncbi_submitter": analyzers,
    ...:                  }
    ...:              }
    ...:          }
    ...:      }
    ...:  ); time.time() - t1
Out[10]: 50.08214974403381
```

So the n-grammification adds a bit to the indexing time, but still pretty reasonable.

Search via Aardvark:

```
Query String (122 chars, cacheable: false):
 FOR d IN genome_attribs_ngram_test
     SEARCH NGRAM_MATCH(d.classification, "UBA18", 0.7, "trigramUTF8lower")
     RETURN d

Execution plan:
 Id   NodeType            Site  Calls   Items   Runtime [s]   Comment
  1   SingletonNode       DBS       2       2       0.00002   * ROOT
  2   EnumerateViewNode   DBS       2     209       0.00558     - FOR d IN genome_attribs_ngram_test SEARCH NGRAM_MATCH(d.`classification`, "UBA18", 0.7, "trigramUTF8lower")   /* view query */
  6   RemoteNode          COOR      6     209       0.02003       - REMOTE
  7   GatherNode          COOR      3     209       0.00781       - GATHER   /* parallel, unsorted */
  3   ReturnNode          COOR      3     209       0.00001       - RETURN d

Indexes used:
 none

Optimization rules applied:
 Id   RuleName
  1   handle-arangosearch-views
  2   scatter-in-cluster
  3   remove-unnecessary-remote-scatter
  4   parallelize-gather

Query Statistics:
 Writes Exec   Writes Ign   Scan Full   Scan Index   Filtered   Peak Mem [b]   Exec Time [s]
           0            0           0          209          0         688128         0.02247

Query Profile:
 Query Stage           Duration [s]
 initializing               0.00000
 parsing                    0.00016
 optimizing ast             0.00001
 loading collections        0.00001
 instantiating plan         0.00003
 optimizing plan            0.00292
 executing                  0.01787
 finalizing                 0.00148
```

209 results

Not much different from `LIKE()`. Probably the DB size isn't big enough to make a difference.

Trying a search with tokens works, but is sensitive to the threshold:

```
Query String (127 chars, cacheable: false):
 FOR d IN genome_attribs_ngram_test
     SEARCH NGRAM_MATCH(d.classification, "hal duran", 0.57, "trigramUTF8lower")
     RETURN d

Execution plan:
 Id   NodeType            Site  Calls   Items   Runtime [s]   Comment
  1   SingletonNode       DBS       2       2       0.00002   * ROOT
  2   EnumerateViewNode   DBS       2     153       0.00488     - FOR d IN genome_attribs_ngram_test SEARCH NGRAM_MATCH(d.`classification`, "hal duran", 0.57, "trigramUTF8lower")   /* view query */
  6   RemoteNode          COOR      6     153       0.01626       - REMOTE
  7   GatherNode          COOR      3     153       0.00705       - GATHER   /* parallel, unsorted */
  3   ReturnNode          COOR      3     153       0.00002       - RETURN d

Indexes used:
 none

Optimization rules applied:
 Id   RuleName
  1   handle-arangosearch-views
  2   scatter-in-cluster
  3   remove-unnecessary-remote-scatter
  4   parallelize-gather

Query Statistics:
 Writes Exec   Writes Ign   Scan Full   Scan Index   Filtered   Peak Mem [b]   Exec Time [s]
           0            0           0          153          0         524288         0.02007

Query Profile:
 Query Stage           Duration [s]
 initializing               0.00000
 parsing                    0.00013
 optimizing ast             0.00001
 loading collections        0.00001
 instantiating plan         0.00003
 optimizing plan            0.00446
 executing                  0.01418
 finalizing                 0.00126

```

153 results, but some of them don't have `hal` as a subtring. Bumping the threshold to 0.58
results in no results.

Reversing the search string to `duran hal` also finds results but the threshold has to be dropped
to 0.42. I assume this is due to n-grams `[an ]` and `[ ha]` not matching anything,
while the n-gram `[ du]` would match but `[al ]` would not in the original search.

We could try tokenizing the input to the n-gram analyzer and see if that helps.
