import re

import jsonlines

import src.common.storage.collection_and_field_names as names
from src.common.hash import md5_string

"""
This module contains helper functions used for gtdb loaders (e.g. gtdb_genome_stats_helper, gtdb_taxa_freq_loader, etc.)
"""


def convert_to_json(docs, outfile):
    """
    Writes list of dictionaries to an argparse File (e.g. argparse.FileType('w')) object in JSON Lines formate.

    Args:
        docs: list of dictionaries
        outfile: an argparse File (e.g. argparse.FileType('w')) object
    """

    with jsonlines.Writer(outfile) as writer:
        writer.write_all(docs)


def parse_genome_id(gtdb_accession):
    """
    Extract the genome id from the GTDB accession field by removing the first 2 characters.

    e.g. GB_GCA_000016605.1 -> GCA_000016605.1
         GB_GCA_000172955.1 -> GCA_000172955.1
    """
    return gtdb_accession[3:]


def init_genome_atrri_doc(kbase_collection, load_version, genome_id):
    """
    Create a document with only a '_key' field in ArangoDB for genome attributes collection.
    """

    # genome id should match below regular expression
    # genome id examples: 'GCA_013331355.1', 'GCF_007846625.1'
    genome_id_regex = r'^[A-Z]{3}_\d+\.\d+$'
    if not re.match(genome_id_regex, genome_id):
        raise ValueError(f'invalid genome id: {genome_id}')

    # The '_key' field for the document should be generated by applying a hash function to a combination of the
    # 'kbase_collection', 'load_version', and 'genome_id' fields.
    doc = {
        names.FLD_ARANGO_KEY: md5_string(f"{kbase_collection}_{load_version}_{genome_id}"),
    }

    return doc
