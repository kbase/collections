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

Metagenome sketches can be >1G per Chris N, so this option won't work for them.

Mongo version is 3.6.12 (which is reasonably close to KBase's version, which is severely out
of date).

```
(collections) crushingismybusiness@andbusinessisgood:~/github/kbase/collections$ time python design/experiments/store_mongo.py store ~/SCIENCE/minhash/sourmash_storage/sourmash/individual/

real	0m17.772s
user	0m3.169s
sys	0m1.526s
```

Checked MongoDB and there were 10K documents with the expected contents.

### Run sourmash stand alone

The signatures were downloaded from mongo into `./sigtemp`.

```
(collections) crushingismybusiness@andbusinessisgood:~/github/kbase/collections$ time sourmash search -n 0 -t 0.5  ~/SCIENCE/minhash/sourmash_storage/sourmash/GCA_018630415.1_ASM1863041v1_genomic.fna.gz.sig ./sigtemp

== This is sourmash version 4.8.2. ==

*snip*

real	0m28.609s
user	0m28.182s
sys	0m1.075s
```

A 2nd run was + ~100ms.

### Download from a just started mongo instance

E.g. no data cached in memory.

```
(collections) crushingismybusiness@andbusinessisgood:~/github/kbase/collections$ rm sigtemp/*
(collections) crushingismybusiness@andbusinessisgood:~/github/kbase/collections$ time python design/experiments/store_mongo.py get ~/SCIENCE/minhash/sourmash_storage/sourmash/GCA_018630415.1_ASM1863041v1_genomic.fna.gz.sig ./sigtemp

== This is sourmash version 4.8.2. ==

*snip*

real	0m34.742s
user	0m29.125s
sys	0m2.116s
```

### Download from a warmed up mongo instance

E.g. some or all of the data is cached in memory.

```
(collections) crushingismybusiness@andbusinessisgood:~/github/kbase/collections$ rm sigtemp/*
(collections) crushingismybusiness@andbusinessisgood:~/github/kbase/collections$ time python design/experiments/store_mongo.py get ~/SCIENCE/minhash/sourmash_storage/sourmash/GCA_018630415.1_ASM1863041v1_genomic.fna.gz.sig ./sigtemp

*snip*

real	0m31.743s
user	0m29.171s
sys	0m2.151s
```

## MongoDB GridFS

Store the files in MongoDB GridFS.

TODO