# Benchmarking storage options for Sourmash sketches.

A quick and dirty benchmark to assess performance of various storage options for Sourmash
sketches.

These tests are not at all statistically meaningful; n=1 in all cases.

Run using Sean J.'s download of GTDB v214. Uses files from a previous analysis
[here](./benchmarking_mash_vs_sourmash.md).

## NFS

Stage files on login1.berkeley.kbase.us and search on docker03 in a docker container.
This prevents the files from being cached in memory by the OS. Prior to any new test the files
need to be deleted and unzipped in different directory to prevent the memory cache from being used.

TODO - need a docker upgrade on docker03

## MongoDB BinData local

Store the files in MongoDB documents as binary data. All work is done on a personal laptop.

Mongo version is 3.6.12 (which is reasonably close to KBase's version, which is severely out
of date).

```
(collections) crushingismybusiness@andbusinessisgood:~/github/kbase/collections$ time python design/experiments/store_mongo.py store ~/SCIENCE/minhash/sourmash_storage/sourmash/individual/

real	0m17.772s
user	0m3.169s
sys	0m1.526s
```

Checked MongoDB and there were 10K documents with the expected contents.

TODO pull down and search the files after shutting down Mongo to drop any caches.

## MongoDB GridFS

Store the files in MongoDB GridFS.

TODO