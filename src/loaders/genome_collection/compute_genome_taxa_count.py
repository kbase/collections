import argparse
import json
import os
from collections import defaultdict

import src.common.storage.collection_and_field_names as names
import src.loaders.common.loader_common_names as loader_common_names
from src.common.gtdb_lineage import (
    parse_gtdb_lineage_string,
    GTDB_RANK_ABBREV_TO_FULL_NAME,
)
from src.common.hash import md5_string
from src.loaders.common.loader_helper import convert_to_json
from src.service.data_products import taxa_count

"""
PROTOTYPE - Prepare genome taxa count data and identical ranks in JSON format for arango import.

usage: compute_genome_taxa_count.py [-h] --load_ver LOAD_VER [--kbase_collection KBASE_COLLECTION] [-o OUTPUT]
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
  -o OUTPUT, --output OUTPUT
                        Output JSON file path.
  --input_source {GTDB,genome_attributes}
                        Input file source
 
e.g. compute_genome_taxa_count.py bac120_taxonomy_r207.tsv ar53_taxonomy_r207.tsv --load_version 207
     compute_genome_taxa_count.py bac120_taxonomy_r207.tsv ar53_taxonomy_r207.tsv --load_version 207 --kbase_collection GTDB
     compute_genome_taxa_count.py bac120_taxonomy_r207.tsv ar53_taxonomy_r207.tsv --load_version 207 --kbase_collection GTDB --output  gtdb_taxa_counts.json
"""

# Default result file name for genome taxa count data and identical ranks for arango import
# Collection and load version information will be prepended to this file name.
GENOME_TAXA_COUNT_FILE = "parsed_genome_taxa_counts.json"

# The source of the input file containing genome taxonomy information to be parsed
# GTDB - taxonomy file downloaded directly from the GTDB website, such as 'bac120_taxonomy_r207.tsv'
# genome_attributes - genome attributes file created by running 'parse_computed_genome_attribs.py' script.
VALID_SOURCE = ['GTDB', 'genome_attributes']


def _parse_lineage_from_line(line, source):
    # parse lineage from one line of file containing genome taxonomy info

    if source == 'GTDB':
        # line from taxonomy file downloaded directly from the GTDB website, such as 'bac120_taxonomy_r207.tsv'
        lineage_str = line.strip().split("\t")[1]
    elif source == 'genome_attributes':
        # line from genome attributes file created by running 'parse_computed_genome_attribs.py' script
        data = json.loads(line)

        if names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE not in data:
            raise ValueError(f'Missing {names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE} attribute from genome attributes file')
        lineage_str = data[names.FLD_GENOME_ATTRIBS_GTDB_LINEAGE]
    else:
        raise ValueError(f'Unsupported input file source: {source}')

    return lineage_str


def _parse_files(load_files, source):
    nodes = defaultdict(lambda: defaultdict(int))
    for load_file in load_files:
        for line in load_file:
            lineage_str = _parse_lineage_from_line(line, source)
            lineage = parse_gtdb_lineage_string(lineage_str)
            for lin in lineage:
                nodes[GTDB_RANK_ABBREV_TO_FULL_NAME[lin.abbreviation]][lin.name] += 1
    return nodes


def _create_count_docs(nodes, kbase_collection, load_version):
    count_docs, identical_ranks = list(), set()
    for rank in nodes:
        for name in nodes[rank]:
            doc = {
                names.FLD_ARANGO_KEY: md5_string(
                    f"{kbase_collection}_{load_version}_{rank}_{name}"
                ),
                names.FLD_COLLECTION_ID: kbase_collection,
                names.FLD_LOAD_VERSION: load_version,
                names.FLD_TAXA_COUNT_RANK: rank,
                names.FLD_TAXA_COUNT_NAME: name,
                names.FLD_TAXA_COUNT_COUNT: nodes[rank][name]
            }
            count_docs.append(doc)

        identical_ranks.add(rank)

    return count_docs, identical_ranks


def _create_rank_docs(kbase_collection, load_version, identical_ranks):
    rank_candidates = list(GTDB_RANK_ABBREV_TO_FULL_NAME.values())

    rank_doc = [{
        names.FLD_ARANGO_KEY: taxa_count.ranks_key(kbase_collection, load_version),
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
    required.add_argument(f'--{loader_common_names.LOAD_VER_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.LOAD_VER_DESCR)

    # Optional argument
    optional.add_argument(f'--{loader_common_names.KBASE_COLLECTION_ARG_NAME}', type=str,
                          default=loader_common_names.DEFAULT_KBASE_COLL_NAME,
                          help=loader_common_names.KBASE_COLLECTION_DESCR)

    optional.add_argument("-o", "--output", type=argparse.FileType('w'),
                          help=f"Output JSON file path.")

    optional.add_argument('--input_source', type=str, choices=VALID_SOURCE, default='GTDB',
                          help='Input file source')

    args = parser.parse_args()
    load_files = args.load_files
    load_version = getattr(args, loader_common_names.LOAD_VER_ARG_NAME)
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    source = args.input_source

    # Close the output file, as the file name will only be referenced later in the code.
    if args.output and not args.output.closed:
        args.output.close()

    print('start parsing input files')
    nodes = _parse_files(load_files, source)

    count_docs, identical_ranks = _create_count_docs(nodes, kbase_collection, load_version)
    rank_doc = _create_rank_docs(kbase_collection, load_version, identical_ranks)
    # Create taxa counts json file
    count_json = args.output.name if args.output else f'{kbase_collection}_{load_version}_{GENOME_TAXA_COUNT_FILE}'
    with open(count_json, 'w') as out_count_json:
        convert_to_json(count_docs, out_count_json)

    # Create identical ranks json file
    root_ext = os.path.splitext(count_json)
    with open(root_ext[0] + '_rank' + root_ext[1], 'w') as out_rank_json:
        convert_to_json(rank_doc, out_rank_json)


if __name__ == "__main__":
    main()
