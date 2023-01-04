"""
PROTOTYPE

This script involves processing computed genome attributes and organizing them into a structured format suitable
for importing into ArangoDB. The resulting JSON file will be used to update (upsert/insert) the database with the parsed
genome attribute data.

Note: The database was previously created using a JSON file generated by the gtdb_genome_attribs_loader.py script.
      It is important to ensure that the arguments "--load_ver" and "--kbase_collection" match the ones used in the
      gtdb_genome_attribs_loader.py script in order to generate the same key for the corresponding Arango document.

usage: parse_computed_genome_attribs.py [-h] --tools TOOLS [TOOLS ...] --load_ver LOAD_VER
                                        [--kbase_collection KBASE_COLLECTION] [--root_dir ROOT_DIR] [-o OUTPUT]
options:
  -h, --help            show this help message and exit

required named arguments:
  --tools TOOLS [TOOLS ...]
                        Extract results from tools. (e.g. gtdb_tk, checkm2, etc.)
  --load_ver LOAD_VER   KBase load version (e.g. r207.kbase.1). This argument should be consistent with the one used by the
                        genome attributes loader script (e.g. gtdb_genome_attribs_loader.py).

optional arguments:
  --kbase_collection KBASE_COLLECTION
                        KBase collection identifier name (default: GTDB). This argument should be consistent with the one
                        used by the genome attributes loader script (e.g. gtdb_genome_attribs_loader.py).
  --root_dir ROOT_DIR   Root directory for the collections project. (default: /global/cfs/cdirs/kbase/collections)
  -o OUTPUT, --output OUTPUT
                        output JSON file path (default: computed_genome_attribs.json)

"""
import argparse
import copy
import os
import sys

import pandas as pd

from src.loaders.common import loader_common_names
from src.loaders.gtdb.gtdb_loader_helper import convert_to_json, init_genome_atrri_doc, merge_docs

# Default result file name for parsed computed genome attributes data for arango import
COMPUTED_GENOME_ATTR_FILE = "computed_genome_attribs.json"

# The following features will be extracted from the CheckM2 result quality_report.tsv file as computed genome attributes
SELECTED_CHECKM2_FEATURES = {'Completeness', 'Contamination'}

# The following features will be extracted from the GTDB-TK summary file
# ('gtdbtk.ar53.summary.tsv' or 'gtdbtk.bac120.summary.tsv') as computed genome attributes
SELECTED_GTDBTK_SUMMARY_FEATURES = {'classification', 'fastani_taxonomy', 'closest_placement_taxonomy',
                                    'pplacer_taxonomy'}

NOTIFICATION = ' This argument should be consistent with the one used by the genome attributes loader script (e.g. gtdb_genome_attribs_loader.py).'


def _locate_dir(root_dir, kbase_collection, load_ver, check_exists=False, tool=''):
    result_dir = os.path.join(root_dir, loader_common_names.COLLECTION_DATA_DIR, kbase_collection, load_ver, tool)

    if check_exists and not (os.path.exists(result_dir) and os.path.isdir(result_dir)):
        raise ValueError(f"Result directory for computed genome attributes of "
                         f"Kbase Collection: {kbase_collection} and Load Version: {load_ver} could not be found.")

    return result_dir


def _read_tsv_as_df(file_path, features, genome_id_col=None):
    # Retrieve the desired fields from a TSV file and return the data in a dataframe

    selected_cols = copy.deepcopy(features)
    if genome_id_col:
        selected_cols.add(genome_id_col)  # genome_id col is used to parse genome id
    df = pd.read_csv(file_path, sep='\t', keep_default_na=False, usecols=selected_cols)

    return df


def _create_doc(row, kbase_collection, load_version, genome_id, selected_features, pre_fix='KBase'):
    # Select specific columns and prepare them for import into Arango

    # NOTE: The selected column names will have a prefix (the tool name) added to them to distinguish them from
    #       the original metadata column names.

    doc = init_genome_atrri_doc(kbase_collection, load_version, genome_id)

    # distinguish the selected fields from the original metadata by adding a common prefix to their names
    doc.update(row[list(selected_features)].rename(lambda x: pre_fix + '_' + x).to_dict())

    return doc


def _gtdbtk_row_to_doc(row, kbase_collection, load_version):
    # Transforms row from GTDBTK summary file 'gtdbtk.ar53.summary.tsv' or 'gtdbtk.bac120.summary.tsv'
    # into ArangoDB collection document

    # parse genome id from the "user_genome" field.
    # the "user_genome" field of GTDBTK summary is the name of input file
    # (e.g. GCA_019057475.1_ASM1905747v1_genomic.fna.gz, GCF_000970085.1_ASM97008v1_genomic.fna.gz).
    genome_id = '_'.join(row.user_genome.split('_')[:2])

    doc = _create_doc(row, kbase_collection, load_version, genome_id, SELECTED_GTDBTK_SUMMARY_FEATURES,
                      pre_fix='gtdb_tk')

    return doc


def gtdb_tk(root_dir, kbase_collection, load_ver):
    """
    Parse and formate result files generated by the GTDB-TK tool.
    """
    gtdb_tk_docs = list()

    result_dir = _locate_dir(root_dir, kbase_collection, load_ver, tool='gtdb_tk')

    # Get the list of directories for batches
    batch_dirs = [d for d in os.listdir(result_dir) if os.path.isdir(os.path.join(result_dir, d))]

    for batch_dir in batch_dirs:

        # retrieve features from summary files
        summary_files = ['gtdbtk.ar53.summary.tsv', 'gtdbtk.bac120.summary.tsv']
        summary_file_exists = False
        for s in summary_files:
            summary_file = os.path.join(result_dir, str(batch_dir), s)

            if os.path.exists(summary_file):
                summary_file_exists = True
                df = _read_tsv_as_df(summary_file, SELECTED_GTDBTK_SUMMARY_FEATURES, genome_id_col='user_genome')
                docs = df.apply(_gtdbtk_row_to_doc, args=(kbase_collection, load_ver), axis=1).to_list()
                gtdb_tk_docs.extend(docs)

        if not summary_file_exists:
            raise ValueError(f'Cannot find summary.tsv file in {os.path.join(result_dir, str(batch_dir))}')

    return gtdb_tk_docs


def _checkm2_row_to_doc(row, kbase_collection, load_version):
    # Transforms row from checkm2 quality_report.tsv into ArangoDB collection document

    # parse genome id from the "Name" field.
    # the "Name" field of checkM2 quality_report.tsv is the base name of input file
    # (e.g. GCA_013331355.1_ASM1333135v1_genomic, GCF_000970085.1_ASM97008v1_genomic).
    genome_id = '_'.join(row.Name.split('_')[:2])

    doc = _create_doc(row, kbase_collection, load_version, genome_id, SELECTED_CHECKM2_FEATURES,
                      pre_fix='checkm2')

    return doc


def checkm2(root_dir, kbase_collection, load_ver):
    """
    Parse and formate result files generated by the CheckM2 tool.
    """
    checkm2_docs = list()

    result_dir = _locate_dir(root_dir, kbase_collection, load_ver, tool='checkm2')

    # Get the list of directories for batches
    batch_dirs = [d for d in os.listdir(result_dir) if os.path.isdir(os.path.join(result_dir, d))]

    for batch_dir in batch_dirs:
        # retrieve checkM2 result from 'quality_report.tsv'
        quality_file = os.path.join(result_dir, str(batch_dir), 'quality_report.tsv')

        if os.path.exists(quality_file):
            df = _read_tsv_as_df(quality_file, SELECTED_CHECKM2_FEATURES, genome_id_col='Name')
            docs = df.apply(_checkm2_row_to_doc, args=(kbase_collection, load_ver), axis=1).to_list()

            checkm2_docs.extend(docs)
        else:
            raise ValueError(f'Cannot find quality_report.tsv file in {os.path.join(result_dir, str(batch_dir))}')

    return checkm2_docs


def main():
    parser = argparse.ArgumentParser(
        description='PROTOTYPE - Generate a JSON file for importing and upserting into ArangoDB by parsing computed '
                    'genome attributes.')
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional arguments')

    # Required flag arguments
    required.add_argument('--tools', required=True, type=str, nargs='+',
                          help='Extract results from tools. (e.g. gtdb_tk, checkm2, etc.)')
    required.add_argument(f'--{loader_common_names.LOAD_VER_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.LOAD_VER_DESCR + NOTIFICATION)

    # Optional arguments
    optional.add_argument(f'--{loader_common_names.KBASE_COLLECTION_ARG_NAME}', type=str,
                          default=loader_common_names.DEFAULT_KBASE_COLL_NAME,
                          help=loader_common_names.KBASE_COLLECTION_DESCR + NOTIFICATION)
    optional.add_argument('--root_dir', type=str, default=loader_common_names.ROOT_DIR,
                          help=f'Root directory for the collections project. (default: {loader_common_names.ROOT_DIR})')
    optional.add_argument("-o", "--output", type=argparse.FileType('w'),
                          default=COMPUTED_GENOME_ATTR_FILE,
                          help=f"output JSON file path (default: {COMPUTED_GENOME_ATTR_FILE})")
    args = parser.parse_args()

    (tools,
     load_ver,
     kbase_collection,
     root_dir) = (args.tools, getattr(args, loader_common_names.LOAD_VER_ARG_NAME),
                  getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME), args.root_dir)

    result_dir = _locate_dir(root_dir, kbase_collection, load_ver, check_exists=True)

    executed_tools = os.listdir(result_dir)
    if set(tools) - set(executed_tools):
        raise ValueError(f'Please ensure that all tools have been successfully executed. '
                         f'Only the following tools have already been run: {executed_tools}')

    docs = list()
    for tool in tools:
        try:
            parse_ops = getattr(sys.modules[__name__], tool)
        except AttributeError as e:
            raise ValueError(f'Please implement parsing method for: [{tool}]') from e

        docs.extend(parse_ops(root_dir, kbase_collection, load_ver))

    docs = merge_docs(docs, '_key')

    with args.output as genome_attribs_json:
        convert_to_json(docs, genome_attribs_json)


if __name__ == "__main__":
    main()
