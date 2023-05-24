# Benchmarking mash

## Sketch all vs sketch 1 by 1 and concatenate

Sketch all of PMI (405 assemblies):
```
login37:~/mash> time /global/homes/g/gaprice/mash/mash-Linux64-v2.3/mash sketch -p 4 -o PMI.sketch -k 19 -s 10000 -l PMI_files_list.txt 
Sketching /global/cfs/cdirs/kbase/collections/collectionssource/PMI/2023.01/68981_327_1/68981_327_1.fa...
*snip*
Writing to PMI.sketch.msh...

real	0m25.914s
user	1m21.199s
sys	0m1.891s
```

Sketch individually:
```
login34:~/mash> module load python
login34:~/mash> time python mash_individual.py 
Sketching /global/cfs/cdirs/kbase/collections/collectionssource/PMI/2023.01/68981_327_1/68981_327_1.fa...
*snip*
Writing to ./PMI_individual_sketches/68981_224_1.fa.msh...

real	1m29.357s
user	1m1.452s
sys	0m2.094s
```

So naively sketching individually seems quite a bit slower. This may not be an issue for our
use case though, if sketches are built on upload and so the user doesn't see the sketch time
either way.

Concatenating sketches is almost instant for the ~405 in PMI
```
login34:~/mash> time mash-Linux64-v2.3/mash paste PMI.sketch.ind.msh PMI_individual_sketches/*
Writing PMI.sketch.ind.msh...

real	0m0.129s
user	0m0.029s
sys	0m0.088s
```

As such, if we sketch everything and store the sketches individually, it should be cheap to
concatenate files of interest when querying.
