import re
from collections import defaultdict

import jsonlines

import src.common.storage.collection_and_field_names as names
from src.common.hash import md5_string

"""
This module contains helper functions used for loaders (e.g. compute_genome_attribs, gtdb_genome_attribs_loader, etc.)
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
    Extract the genome id from the GTDB accession field by removing the first 3 characters.

    e.g. GB_GCA_000016605.1 -> GCA_000016605.1
         GB_GCA_000172955.1 -> GCA_000172955.1
    """
    return gtdb_accession[3:]


def copy_column(df, existing_col, new_col):
    """
    Copy existing column to new column.
    If the new column already exists, it will be overwritten.

    # TODO add options for modifying the data in the copied column during the copying process.
    """
    if existing_col not in df:
        raise ValueError(f'Error: The {existing_col} column does not exist in the DataFrame.')

    df[new_col] = df[existing_col]


def merge_docs(docs, key_name):
    """
    merge dictionaries with the same key value in a list of dictionaries
    """
    merged = defaultdict(dict)

    for d in docs:
        key_value = d[key_name]
        merged[key_value].update(d)

    return merged.values()


def init_genome_atrri_doc(kbase_collection, load_version, genome_id):
    """
    Initialize a dictionary with a single field, '_key',
    which will be used as the primary key for the genome attributes collection in ArangoDB.
    """

    # The '_key' field for the document should be generated by applying a hash function to a combination of the
    # 'kbase_collection', 'load_version', and 'genome_id' fields.
    doc = {
        names.FLD_ARANGO_KEY: md5_string(f"{kbase_collection}_{load_version}_{genome_id}"),
        names.FLD_COLLECTION_ID: kbase_collection,
        names.FLD_LOAD_VERSION: load_version,
        names.FLD_GENOME_ATTRIBS_KBASE_GENOME_ID: genome_id,  # Begin with the input genome_id, though it may be altered by the calling script.
        names.FLD_GENOME_ATTRIBS_MATCHES_SELECTIONS: []  # for saving matches and selections
    }

    return doc
