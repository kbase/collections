import json
import os
import socket
import subprocess
import time
from collections import defaultdict

import jsonlines

import src.common.storage.collection_and_field_names as names
from src.common.hash import md5_string
from src.loaders.common.loader_common_names import DOCKER_HOST, JSON_KEYS

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
        raise ValueError(
            f"Error: The {existing_col} column does not exist in the DataFrame."
        )

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
        names.FLD_KBASE_ID: genome_id,  # Begin with the input genome_id, though it may be altered by the calling script.
        names.FLD_MATCHES_SELECTIONS: []  # for saving matches and selections
    }

    return doc


def get_token(token_filepath):
    """
    Get token from a file path. 
    """
    with open(os.path.expanduser(token_filepath), "r") as f:
        token = f.readline().strip()
    return token


def start_podman_service(uid: int):
    """
    Start podman service. Used by workspace_downloader.py script.

    uid - the integer unix user ID of the user running the service.
    """
    # TODO find out the right way to check if a podman service is running
    command = ["podman", "system", "service", "-t", "0"]
    proc = subprocess.Popen(command)
    time.sleep(1)
    return_code = proc.poll()
    if return_code:
        raise ValueError(f'The command {command} failed with return code {return_code}')
    os.environ["DOCKER_HOST"] = DOCKER_HOST.format(uid)
    return proc


def is_upa_info_complete(output_dir: str, upa: str):
    """
    Check whether an UPA needs to be downloaded or not by loading the metadata file.
    Make sure it has all the right keys.
    """
    fa_path = os.path.join(output_dir, upa, upa + ".fa")
    meta_path = os.path.join(output_dir, upa, upa + ".meta")
    if not os.path.exists(fa_path) or not os.path.exists(meta_path):
        return False
    try:
        with open(meta_path, "r") as json_file:
            data = json.load(json_file)
    except:
        return False
    if not set(JSON_KEYS).issubset(set(data.keys())):
        return False
    return True


def get_ip():
    """
    Get current ip address
    """
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return ip
