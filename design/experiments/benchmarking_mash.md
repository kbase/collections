# Benchmarking mash

## PMI

### Sketch all vs sketch 1 by 1 and concatenate

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

## GTDB vs Refseq

### Sketch 10k

```
gaprice@perlmutter:login34:~/mash> tail GTDB_files_list.txt 
/global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207/GB_GCA_016839275.1/GCA_016839275.1_ASM1683927v1_genomic.fna.gz
/global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207/RS_GCF_000798115.1/GCF_000798115.1_Escherichia_coli_CVM_N36393PS_v._1.0_genomic.fna.gz
/global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207/RS_GCF_003027185.1/GCF_003027185.1_ASM302718v1_genomic.fna.gz
/global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207/RS_GCF_012027515.1/GCF_012027515.1_ASM1202751v1_genomic.fna.gz
/global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207/RS_GCF_000687125.1/GCF_000687125.1_ASM68712v1_genomic.fna.gz
/global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207/RS_GCF_002768995.1/GCF_002768995.1_ASM276899v1_genomic.fna.gz
/global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207/RS_GCF_900008835.1/GCF_900008835.1_ED195_contigs_genomic.fna.gz
/global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207/RS_GCF_009602405.1/GCF_009602405.1_ASM960240v1_genomic.fna.gz
/global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207/GB_GCA_018992705.1/GCA_018992705.1_ASM1899270v1_genomic.fna.gz
/global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207/RS_GCF_001677865.2/GCF_001677865.2_ASM167786v2_genomic.fna.gz
gaprice@perlmutter:login34:~/mash>  time /global/homes/g/gaprice/mash/mash-Linux64-v2.3/mash sketch -p 4 -o GTDB_10k.sketch -k 19 -s 10000 -l GTDB_files_list.txt
*snip*
Sketching /global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207/RS_GCF_001677865.2/GCF_001677865.2_ASM167786v2_genomic.fna.gz...
Writing to GTDB_10k.sketch.msh...

real	5m9.801s
user	16m35.507s
sys	0m8.056s
```

### Calculate distances for 10k vs 100k

```
gaprice@perlmutter:login34:/global/cfs/cdirs/kbase/homology/refseq> time /global/homes/g/gaprice/mash/mash-Linux64-v2.3/mash dist -d 0.5 ~/mash/GTDB_10k.sketch.msh RefSeq_10000_19.msh > ~/mash/GTDB_10K_vs_Refseq.out
^C
real	56m32.348s
user	56m28.831s
sys	0m37.972s
```
After termination:
```
gaprice@perlmutter:login34:~/mash> wc -l GTDB_10K_vs_Refseq.out 
9998348 GTDB_10K_vs_Refseq.out
```

So about 100 sequences of the 10K per hour, and so we can expect 100 hours of processing time
to run the full 10K sequences for 1B distances.
