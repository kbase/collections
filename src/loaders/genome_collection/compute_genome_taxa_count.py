import argparse
import json

import src.common.storage.collection_and_field_names as names
import src.loaders.common.loader_common_names as loader_common_names
from src.common.gtdb_lineage import (
    GTDBRank,
    GTDBTaxaCount,
)
from src.common.storage.db_doc_conversions import (
    collection_load_version_key,
    taxa_node_count_to_doc,
)
from src.loaders.common.loader_helper import create_import_files

"""
PROTOTYPE - Prepare genome taxa count data and identical ranks in JSON format for arango import.

usage: compute_genome_taxa_count.py [-h] --load_ver LOAD_VER [--kbase_collection KBASE_COLLECTION] [--root_dir ROOT_DIR]
                                    [--input_source {GTDB,genome_attributes}]
                                    load_files [load_files ...]

options:
  -h, --help            show this help message and exit

required named arguments:
  load_files            Files containing genome taxonomy (e.g. ar53_taxonomy_r207.tsv, computed_genome_attribs.json)
  --load_ver LOAD_VER   KBase load version (e.g. r207.kbase.1).

optional arguments:
  --kbase_collection KBASE_COLLECTION
                        KBase collection identifier name (default: GTDB).
  --root_dir ROOT_DIR   Root directory for the collections project (default: /global/cfs/cdirs/kbase/collections)
  --input_source {GTDB,genome_attributes}
                        Input file source
 
e.g. compute_genome_taxa_count.py bac120_taxonomy_r207.tsv ar53_taxonomy_r207.tsv --load_ver 207
     compute_genome_taxa_count.py bac120_taxonomy_r207.tsv ar53_taxonomy_r207.tsv --load_ver 207 --kbase_collection GTDB
     compute_genome_taxa_count.py ENIGMA_2023.01_checkm2_gtdb_tk_computed_genome_attribs.jsonl --load_ver 2023.01 --kbase_collection ENIGMA --input_source genome_attributes
"""

# The source of the input file containing genome taxonomy information to be parsed
# GTDB - taxonomy file downloaded directly from the GTDB website, such as 'bac120_taxonomy_r207.tsv'
# genome_attributes - genome attributes file created by running 'parse_tool_results.py' script.
VALID_SOURCE = ['GTDB', 'genome_attributes']


def _parse_lineage_from_line(line, source):
    # parse lineage from one line of file containing genome taxonomy info

    if source == 'GTDB':
        # line from taxonomy file downloaded directly from the GTDB website, such as 'bac120_taxonomy_r207.tsv'
        lineage_str = line.strip().split("\t")[1]
    elif source == 'genome_attributes':
        # line from genome attributes file created by running 'parse_tool_results.py' script
        data = json.loads(line)

        if names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE not in data:
            raise ValueError(f'Missing {names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE} attribute from genome attributes file')
        lineage_str = data[names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE]
    else:
        raise ValueError(f'Unsupported input file source: {source}')

    return lineage_str


def _parse_files(load_files, source):
    nodes = GTDBTaxaCount()
    for load_file in load_files:
        for line in load_file:
            lineage_str = _parse_lineage_from_line(line, source)
            nodes.add(lineage_str)
    return nodes


def _create_count_docs(nodes, kbase_collection, load_version):
    count_docs, identical_ranks = list(), set()
    for node in nodes:
        doc = taxa_node_count_to_doc(kbase_collection, load_version, node)
        count_docs.append(doc)

        identical_ranks.add(node.rank.value)

    return count_docs, identical_ranks


def _create_rank_docs(kbase_collection, load_version, identical_ranks):
    rank_candidates = [r.value for r in GTDBRank]

    rank_doc = [{
        names.FLD_ARANGO_KEY: collection_load_version_key(kbase_collection, load_version),
        names.FLD_COLLECTION_ID: kbase_collection,
        names.FLD_LOAD_VERSION: load_version,
        names.FLD_TAXA_COUNT_RANKS: [r for r in rank_candidates if r in identical_ranks]}]

    return rank_doc


def main():
    parser = argparse.ArgumentParser(
        description='PROTOTYPE - Prepare genome taxa count data and identical ranks in JSON format for arango import.'
    )
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional arguments')

    # Required positional argument
    required.add_argument('load_files', type=argparse.FileType('r'), nargs='+',
                          help='Files containing genome taxonomy (e.g. ar53_taxonomy_r207.tsv, '
                               'computed_genome_attribs.json)')

    # Required flag argument
    required.add_argument(f'--{loader_common_names.KBASE_COLLECTION_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.KBASE_COLLECTION_DESCR)
    required.add_argument(f'--{loader_common_names.LOAD_VER_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.LOAD_VER_DESCR)

    # Optional argument
    optional.add_argument(
        f"--{loader_common_names.ENV_ARG_NAME}",
        type=str,
        choices=loader_common_names.KB_ENV + [loader_common_names.DEFAULT_ENV],
        default='PROD',
        help="Environment containing the data to be processed. (default: PROD)",
    )
    optional.add_argument('--root_dir', type=str, default=loader_common_names.ROOT_DIR,
                          help=f'Root directory for the collections project (default: {loader_common_names.ROOT_DIR})')

    optional.add_argument('--input_source', type=str, choices=VALID_SOURCE, default='GTDB',
                          help='Input file source')

    args = parser.parse_args()
    load_files = args.load_files
    root_dir = args.root_dir
    load_version = getattr(args, loader_common_names.LOAD_VER_ARG_NAME)
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    env = getattr(args, loader_common_names.ENV_ARG_NAME)
    source = args.input_source

    print('start parsing input files')
    nodes = _parse_files(load_files, source)

    count_docs, identical_ranks = _create_count_docs(nodes, kbase_collection, load_version)
    rank_doc = _create_rank_docs(kbase_collection, load_version, identical_ranks)

    # Create taxa counts jsonl file
    count_jsonl = f'{kbase_collection}_{load_version}_{names.COLL_TAXA_COUNT}.jsonl'
    create_import_files(root_dir, env, count_jsonl, count_docs)

    # Create identical ranks jsonl file
    count_ranks_jsonl = f'{kbase_collection}_{load_version}_{names.COLL_TAXA_COUNT_RANKS}.jsonl'
    create_import_files(root_dir, env, count_ranks_jsonl, rank_doc)


if __name__ == "__main__":
    main()
