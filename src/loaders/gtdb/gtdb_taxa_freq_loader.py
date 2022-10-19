import argparse
import hashlib
from collections import defaultdict

import jsonlines

"""
PROTOTYPE

Prepare GTDB taxa frequency data in JSON format for arango import.

usage: gtdb_taxa_freq_loader.py [-h] --load_version
                                LOAD_VERSION
                                [--kbase_collection KBASE_COLLECTION]
                                [-o OUTPUT]
                                load_files
                                [load_files ...]
 
e.g. gtdb_taxa_freq_loader.py bac120_taxonomy_r207.tsv ar53_taxonomy_r207.tsv --load_version 207
     gtdb_taxa_freq_loader.py bac120_taxonomy_r207.tsv ar53_taxonomy_r207.tsv --load_version 207 --kbase_collection gtdb
     gtdb_taxa_freq_loader.py bac120_taxonomy_r207.tsv ar53_taxonomy_r207.tsv --load_version 207 --kbase_collection gtdb --output  gtdb_taxa_frequency.json
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


def _create_docs(nodes, load_version, kbase_collection):
    docs = list()

    for rank in nodes:
        for name in nodes[rank]:
            doc = {
                "_key": hashlib.md5(
                    f"{kbase_collection}_{load_version}_{rank}_{name}".encode('utf-8')
                ).hexdigest(),
                "collection": kbase_collection,
                "load_version": load_version,
                "rank": rank,
                "name": name,
                "count": nodes[rank][name]
            }
            docs.append(doc)

    return docs


def _convert_to_json(docs, outfile):
    writer = jsonlines.Writer(outfile)
    writer.write_all(docs)
    writer.close()


def main():
    parser = argparse.ArgumentParser(description='PROTOTYPE - Prepare GTDB taxa frequency data in JSON format for '
                                                 'arango import.')

    # Required positional argument
    parser.add_argument('load_files', type=argparse.FileType('r'), nargs='+',
                        help='GTDB taxonomy files')

    # Required flag argument
    parser.add_argument(
        '--load_version', required=True, type=str, nargs=1, help='KBase load version')

    # Optional argument
    parser.add_argument('--kbase_collection', type=str, default='gtdb',
                        help='kbase collection identifier name (default: gtdb)')
    parser.add_argument("-o", "--output", type=argparse.FileType('w'), default='gtdb_taxa_frequency.json',
                        help="output JSON file path (default: gtdb_taxa_frequency.json")

    args = parser.parse_args()
    load_files, load_version, kbase_collection = args.load_files, args.load_version[0], args.kbase_collection

    print('start parsing input files')
    nodes = _parse_files(load_files)
    docs = _create_docs(nodes, load_version, kbase_collection)
    _convert_to_json(docs, args.output)


if __name__ == "__main__":
    main()
