import argparse

import pandas as pd

import src.common.storage.collection_and_field_names as names
import src.loaders.common.loader_common_names as loader_common_names
import src.loaders.gtdb.gtdb_loader_helper as loader_helper
from src.common.storage.collection_and_field_names import (
    FLD_COLLECTION_ID,
    FLD_LOAD_VERSION,
    FLD_GENOME_ATTRIBS_GTDB_LINEAGE,
)

"""
PROTOTYPE - Prepare GTDB genome statistics data in JSON format for arango import.

This script parses genome features from the GTDB metadata files. Those features should be put into the global variable 
SELECTED_FEATURES.

NOTE: The Document Key ("_key") in Arango DB is generated by applying a hash function to a string consisting of the 
kbase_collection, load_version, and genome id parsed from the accession field from the GTDB metadata file. 
(e.g. GTDB_r207.kbase.1_GCA_000016605.1, GTDB_r207.kbase.1_GCA_000169995.1)

usage: gtdb_genome_attribs_loader.py [-h] --load_ver LOAD_VER [--kbase_collection KBASE_COLLECTION] [-o OUTPUT]
                                     load_files [load_files ...]

options:
  -h, --help            show this help message and exit

required named arguments:
  load_files            GTDB genome metadata files
  --load_ver LOAD_VER   KBase load version. (e.g. r207.kbase.1)

optional arguments:
  --kbase_collection KBASE_COLLECTION
                        kbase collection identifier name (default: loader_common_names.DEFAULT_KBASE_COLL_NAME)
  -o OUTPUT, --output OUTPUT
                        output JSON file path (default: gtdb_genome_attribs.json)


e.g. gtdb_genome_attribs_loader.py bac120_metadata_r207.tsv ar53_metadata_r207.tsv --load_version r207.kbase.1
     gtdb_genome_attribs_loader.py bac120_metadata_r207.tsv ar53_metadata_r207.tsv --load_version r207.kbase.1 --kbase_collection GTDB
     gtdb_genome_attribs_loader.py bac120_metadata_r207.tsv ar53_metadata_r207.tsv --load_version r207.kbase.1 --kbase_collection GTDB --output  gtdb_genome_attribs.json
"""

# Default result file name for GTDB genome attributes data for arango import
GTDB_GENOME_ATTR_FILE = "gtdb_genome_attribs.json"

# Note that FLD_GENOME_ATTRIBS_GTDB_LINEAGE is required for some matchers to function.
# Since it's always part of the GTDB file we don't bother independently checking for its
# presence
"""
The following features will be extracted from the GTDB metadata file 
(e.g. ar122_metadata_r202.tsv and bac120_metadata_r202.tsv)
"""
SELECTED_FEATURES = {'accession', 'checkm_completeness', 'checkm_contamination', 'checkm_marker_count',
                     'checkm_marker_lineage', 'checkm_marker_set_count', 'contig_count', 'gc_count', 'gc_percentage',
                     'genome_size', FLD_GENOME_ATTRIBS_GTDB_LINEAGE, 'longest_contig', 'longest_scaffold', 'mean_contig_length',
                     'mean_scaffold_length', 'mimag_high_quality', 'mimag_low_quality', 'mimag_medium_quality',
                     'n50_contigs', 'n50_scaffolds', 'ncbi_assembly_level', 'ncbi_assembly_name', 'ncbi_bioproject',
                     'ncbi_biosample', 'ncbi_country', 'ncbi_date', 'ncbi_genbank_assembly_accession',
                     'ncbi_genome_category', 'ncbi_isolate', 'ncbi_isolation_source', 'ncbi_lat_lon',
                     'ncbi_organism_name',
                     'ncbi_seq_rel_date', 'ncbi_species_taxid', 'ncbi_strain_identifiers', 'ncbi_submitter',
                     'ncbi_taxid',
                     'ncbi_taxonomy_unfiltered', 'protein_count', 'scaffold_count', 'ssu_count', 'ssu_length',
                     'trna_aa_count', 'trna_count', 'trna_selenocysteine_count'}

# Copy the value of the following column to a new column FLD_GENOME_ATTRIBS_KBASE_GENOME_ID,
# which is the sort key for the genome attribute collection.
KBASE_GENOME_ID_COL = 'accession'


def _parse_from_metadata_file(load_files, exist_features, additional_features=None):
    """
    Fetches certain columns (combination of exist_features and additional_features) from GTDB metadata file
    and saves result as a pandas data from
    """

    if additional_features is None:
        additional_features = {}
        
    frames = [pd.read_csv(load_file, sep='\t', header=0, keep_default_na=False,
                          usecols=exist_features.union(additional_features)) for load_file in load_files]
    df = pd.concat(frames, ignore_index=True)

    return df


def _row_to_doc(row, kbase_collection, load_version):
    """
    Transforms row (from a dataframe) into ArangoDB collection document
    """
    # parse genome id
    genome_id = loader_helper.parse_genome_id(row.accession)
    doc = loader_helper.init_genome_atrri_doc(kbase_collection, load_version, genome_id)

    doc[FLD_COLLECTION_ID] = kbase_collection
    doc[FLD_LOAD_VERSION] = load_version

    doc.update(row.to_dict())

    return doc


def _df_to_docs(df, kbase_collection, load_version):
    """
    Convert a DataFrame into a list of documents that will be imported into the genome attributes collection.
    """

    # Create the FLD_GENOME_ATTRIBS_KBASE_GENOME_ID column by copying the KBASE_GENOME_ID_COL column
    loader_helper.copy_column(df, KBASE_GENOME_ID_COL, names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID)
    docs = df.apply(_row_to_doc, args=(kbase_collection, load_version), axis=1).to_list()

    return docs


def main():
    parser = argparse.ArgumentParser(
        description='PROTOTYPE - Prepare GTDB genome statistics data in JSON format for arango import.')
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional arguments')

    # Required positional argument
    required.add_argument('load_files', type=argparse.FileType('r'), nargs='+',
                          help='GTDB genome metadata files (e.g. ar53_metadata_r207.tsv)')

    # Required flag argument
    required.add_argument(f'--{loader_common_names.LOAD_VER_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.LOAD_VER_DESCR)

    # Optional argument
    optional.add_argument(f'--{loader_common_names.KBASE_COLLECTION_ARG_NAME}', type=str,
                          default=loader_common_names.DEFAULT_KBASE_COLL_NAME,
                          help=loader_common_names.KBASE_COLLECTION_DESCR)
    optional.add_argument("-o", "--output", type=argparse.FileType('w'),
                          default=GTDB_GENOME_ATTR_FILE,
                          help=f"output JSON file path (default: {GTDB_GENOME_ATTR_FILE}")

    args = parser.parse_args()
    load_files, load_version, kbase_collection = (args.load_files,
                                                  getattr(args, loader_common_names.LOAD_VER_ARG_NAME),
                                                  getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME))

    print('start parsing input files')
    df = _parse_from_metadata_file(load_files, SELECTED_FEATURES)
    docs = _df_to_docs(df, kbase_collection, load_version)

    with args.output as genome_attribs_json:
        loader_helper.convert_to_json(docs, genome_attribs_json)


if __name__ == "__main__":
    main()
