import argparse
import os
from collections import defaultdict

from gtdb_loader_helper import convert_to_json

from src.common.hash import md5_string
import src.common.storage.collection_and_field_names as names
import src.loaders.common.loader_common_names as loader_common_names
from src.service.data_products import taxa_count

"""
PROTOTYPE

Prepare GTDB taxa count data and identical ranks in JSON format for arango import.

usage: gtdb_taxa_count_loader.py [-h] --load_ver LOAD_VER [--kbase_collection KBASE_COLLECTION] [-o OUTPUT]
                                 load_files [load_files ...]

options:
  -h, --help            show this help message and exit

required named arguments:
  load_files            GTDB taxonomy files (e.g. ar53_taxonomy_r207.tsv)
  --load_ver LOAD_VER   KBase load version. (e.g. r207.kbase.1)

optional arguments:
  --kbase_collection KBASE_COLLECTION
                        kbase collection identifier name (default: GTDB)
  -o OUTPUT, --output OUTPUT
                        output JSON file path (default: gtdb_taxa_counts.json
 
e.g. gtdb_taxa_count_loader.py bac120_taxonomy_r207.tsv ar53_taxonomy_r207.tsv --load_version 207
     gtdb_taxa_count_loader.py bac120_taxonomy_r207.tsv ar53_taxonomy_r207.tsv --load_version 207 --kbase_collection GTDB
     gtdb_taxa_count_loader.py bac120_taxonomy_r207.tsv ar53_taxonomy_r207.tsv --load_version 207 --kbase_collection GTDB --output  gtdb_taxa_counts.json
"""

_ABBRV_SPECIES = "s"

_TAXA_TYPES = {
    "d": "domain",
    "p": "phylum",
    "c": "class",
    "o": "order",
    "f": "family",
    "g": "genus",
    _ABBRV_SPECIES: "species",
}


def _get_lineage(linstr):
    ln = linstr.split(";")
    ret = []
    for lin in ln:
        taxa_abbrev, taxa_name = lin.split("__")
        ret.append({"abbrev": taxa_abbrev, "name": taxa_name})
    if ret[-1]["abbrev"] != _ABBRV_SPECIES:
        raise ValueError(f"Lineage {linstr} does not end with species")
    return ret


def _parse_files(load_files):
    nodes = defaultdict(lambda: defaultdict(int))
    for load_file in load_files:
        for line in load_file:
            lineage = line.strip().split("\t")[1]
            lineage = _get_lineage(lineage)
            for lin in lineage:
                nodes[_TAXA_TYPES[lin['abbrev']]][lin['name']] += 1
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
    rank_candidates = list(_TAXA_TYPES.values())

    rank_doc = [{
        names.FLD_ARANGO_KEY: taxa_count.ranks_key(kbase_collection, load_version),
        names.FLD_COLLECTION_ID: kbase_collection,
        names.FLD_LOAD_VERSION: load_version,
        names.FLD_TAXA_COUNT_RANKS: [r for r in rank_candidates if r in identical_ranks]}]

    return rank_doc


def main():
    parser = argparse.ArgumentParser(
        description='PROTOTYPE - Prepare GTDB taxa count data in JSON format for arango import.'
    )
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional arguments')

    # Required positional argument
    required.add_argument('load_files', type=argparse.FileType('r'), nargs='+',
                          help='GTDB taxonomy files (e.g. ar53_taxonomy_r207.tsv)')

    # Required flag argument
    required.add_argument(f'--{loader_common_names.LOAD_VER_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.LOAD_VER_DESCR)

    # Optional argument
    optional.add_argument(f'--{loader_common_names.KBASE_COLLECTION_ARG_NAME}', type=str,
                          default=names.DEFAULT_KBASE_COLL_NAME,
                          help=loader_common_names.KBASE_COLLECTION_DESCR)

    optional.add_argument("-o", "--output", type=argparse.FileType('w'),
                          default=loader_common_names.GTDB_TAXA_COUNT_FILE,
                          help=f"output JSON file path (default: {loader_common_names.GTDB_TAXA_COUNT_FILE}")

    args = parser.parse_args()
    load_files, load_version, kbase_collection = (args.load_files,
                                                  getattr(args, loader_common_names.LOAD_VER_ARG_NAME),
                                                  getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME))

    print('start parsing input files')
    nodes = _parse_files(load_files)

    count_docs, identical_ranks = _create_count_docs(nodes, kbase_collection, load_version)
    rank_doc = _create_rank_docs(kbase_collection, load_version, identical_ranks)
    # Create taxa counts json file
    count_json = args.output
    with count_json as out_count_json:
        convert_to_json(count_docs, out_count_json)

    # Create identical ranks json file
    root_ext = os.path.splitext(count_json.name)
    with open(root_ext[0] + '_rank' + root_ext[1], 'w') as out_rank_json:
        convert_to_json(rank_doc, out_rank_json)


if __name__ == "__main__":
    main()
