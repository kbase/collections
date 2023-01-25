# Generate Genome Attributes

This document describes the steps and scripts used to generate attributes of a genome. 
The scripts used in the process are located in the `src/loaders` directory.

1. Retrieve Source Genome
   * The first step is to download the genome files from the NCBI FTP server using the script 
   `ncbi_downloader/ncbi_downloader.py`. 
   * It is possible to obtain source genome files from sources other than NCBI. 
   However, by default, the source genome files used in this process are stored on the NERSC server at the path
   `/global/cfs/cdirs/kbase/collections`
2. Compute Genome Attributes
   * The next step is to execute tools to compute various attributes of the genome using the script 
   `genome_attributes/compute_genome_attribs.py`. 
   * This script currently supports two tools for attribute computation:
   [GTDB-TK](https://ecogenomics.github.io/GTDBTk/index.html) 
   and [CheckM2](https://github.com/chklovski/CheckM2)
3. Parse Tool Results
   * After the attribute computation tools have been executed, the results need to be parsed and organized 
   into a format that is suitable for importing into ArangoDB. This is done using the script 
   `genome_attributes/parse_computed_genome_attribs.py`. 


For usage instructions for each script, please refer to the help option (-h) of the script.