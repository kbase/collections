"""
Module that provides a list of common names, along with their corresponding descriptions, to be utilized
by the loaders programs located in the src/loaders directory.
"""

import src.common.storage.collection_and_field_names as names

# Arguments Descriptions

# Name for load version argument
LOAD_VER_ARG_NAME = "load_ver"
# Description of the --load_ver argument in various loaders programs.
LOAD_VER_DESCR = "KBase load version (e.g. r207.kbase.1)."

# Name for kbase collection argument
KBASE_COLLECTION_ARG_NAME = "kbase_collection"
# Description of the --kbase_collection argument in various loaders programs.
KBASE_COLLECTION_DESCR = f'kbase collection identifier name (default: {names.DEFAULT_KBASE_COLL_NAME}).'

# Default result file name for GTDB genome attributes data for arango import
# (created by loaders/gtdb/gtdb_genome_attribs_loader.py script)
GTDB_GENOME_ATTR_FILE = "gtdb_genome_attribs.json"
# Default result file name for GTDB taxa count data and identical ranks for arango import
# (created by loaders/gtdb/gtdb_taxa_count_loader.py script)
GTDB_TAXA_COUNT_FILE = "gtdb_taxa_counts.json"
# Default result file name for parsed computed GTDB genome attributes data for arango import
# (created by loaders/gtdb/parse_computed_genome_attribs.py script)
GTDB_COMPUTED_GENOME_ATTR_FILE = "gtdb_computed_genome_attribs.json"

"""
File structure at NERSC for loader programs
"""

ROOT_DIR = '/global/cfs/cdirs/kbase/collections'  # root directory for the collections project
SOURCE_DATA_DIR = 'sourcedata'  # subdirectory for all source data (e.g. GTDB genome files)
COLLECTION_DATA_DIR = 'collectionsdata'  # subdirectory for collected data (e.g. computed genome attributes)

"""
The following features will be extracted from the GTDB metadata file 
(e.g. ar122_metadata_r202.tsv and bac120_metadata_r202.tsv)
"""
SELECTED_FEATURES = {'accession', 'checkm_completeness', 'checkm_contamination', 'checkm_marker_count',
                     'checkm_marker_lineage', 'checkm_marker_set_count', 'contig_count', 'gc_count', 'gc_percentage',
                     'genome_size', 'gtdb_taxonomy', 'longest_contig', 'longest_scaffold', 'mean_contig_length',
                     'mean_scaffold_length', 'mimag_high_quality', 'mimag_low_quality', 'mimag_medium_quality',
                     'n50_contigs', 'n50_scaffolds', 'ncbi_assembly_level', 'ncbi_assembly_name', 'ncbi_bioproject',
                     'ncbi_biosample', 'ncbi_country', 'ncbi_date', 'ncbi_genbank_assembly_accession',
                     'ncbi_genome_category', 'ncbi_isolate', 'ncbi_isolation_source', 'ncbi_lat_lon',
                     'ncbi_organism_name',
                     'ncbi_seq_rel_date', 'ncbi_species_taxid', 'ncbi_strain_identifiers', 'ncbi_submitter',
                     'ncbi_taxid',
                     'ncbi_taxonomy_unfiltered', 'protein_count', 'scaffold_count', 'ssu_count', 'ssu_length',
                     'trna_aa_count', 'trna_count', 'trna_selenocysteine_count'}