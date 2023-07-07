import argparse
import json
import os
import socket
import subprocess
import time
from collections import defaultdict
from contextlib import closing
from pathlib import Path
from typing import Any

import jsonlines

import src.common.storage.collection_and_field_names as names
from src.common.storage.db_doc_conversions import collection_data_id_key
from src.loaders.common import loader_common_names
from src.loaders.common.loader_common_names import (
    DOCKER_HOST,
    FATAL_ERROR,
    FATAL_STACKTRACE,
    FATAL_TOOL,
    IMPORT_DIR,
    KB_AUTH_TOKEN,
    SOURCE_METADATA_FILE_KEYS,
)

"""
This module contains helper functions used for loaders (e.g. compute_genome_attribs, gtdb_genome_attribs_loader, etc.)
"""


def form_source_dir(
        root_dir: str,
        env: str,
        kbase_collection: str,
        source_ver: str
) -> Path:
    """
    Form the path to the collections source data directory.
    (e.g. root_dir/collectionssource/env/kbase_collection/source_ver)
    """

    return Path(root_dir) / loader_common_names.COLLECTION_SOURCE_DIR / env / kbase_collection / source_ver


def convert_to_json(docs, outfile):
    """
    Writes list of dictionaries to a file-like-object in JSON Lines format.

    Args:
        docs: list of dictionaries
        outfile: an argparse File (e.g. argparse.FileType('w')) object
    """

    with jsonlines.Writer(outfile) as writer:
        writer.write_all(docs)


def create_import_dir(root_dir: str, env: str) -> Path:
    """
    Create the import directory for the given environment.
    """
    import_dir = Path(root_dir, IMPORT_DIR, env)
    os.makedirs(import_dir, exist_ok=True)

    return import_dir


def create_import_files(root_dir: str, env: str, file_name: str, docs: list[dict[str, Any]]):
    """
    Create and save the data documents as JSONLines file to the import directory.
    """
    import_dir = create_import_dir(root_dir, env)

    file_path = os.path.join(import_dir, file_name)
    print(f'Creating JSONLines import file: {file_path}')
    with open(file_path, 'w') as f:
        convert_to_json(docs, f)


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
        # TODO add option to handle key collisions
        merged[key_value].update(d)

    return merged.values()


def init_row_doc(kbase_collection, load_version, data_id):
    """
    Initialize a dictionary with a single field, '_key',
    which will be used as the primary key for collections in ArangoDB.
    """

    # The '_key' field for the document should be generated by applying a hash function to a combination of the
    # 'kbase_collection', 'load_version', and 'data_id' fields.
    doc = {
        names.FLD_ARANGO_KEY: collection_data_id_key(
            kbase_collection, load_version, data_id
        ),
        names.FLD_COLLECTION_ID: kbase_collection,
        names.FLD_LOAD_VERSION: load_version,
        names.FLD_KBASE_ID: data_id,
        # Begin with the input data_id, though it may be altered by the calling script.
        names.FLD_MATCHES_SELECTIONS: [],  # for saving matches and selections
    }

    return doc


def get_token(token_filepath):
    """
    Get the token from the file and then from the env if it's not in the file.
    """
    if token_filepath:
        with open(os.path.expanduser(token_filepath), "r") as f:
            token = f.readline().strip()
    else:
        token = os.environ.get(KB_AUTH_TOKEN)
    if not token:
        raise ValueError(
            f"Need to provide a token in the {KB_AUTH_TOKEN} "
            + f"environment variable or as --token_filepath argument to the CLI"
        )
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
        raise ValueError(f"The command {command} failed with return code {return_code}")
    os.environ["DOCKER_HOST"] = DOCKER_HOST.format(uid)
    return proc


def is_upa_info_complete(upa_dir: str):
    """
    Check whether an UPA needs to be downloaded or not by loading the metadata file.
    Make sure it has all the right keys.
    """
    upa = os.path.basename(upa_dir)
    fa_path = os.path.join(upa_dir, upa + ".fa")
    meta_path = os.path.join(upa_dir, upa + ".meta")
    if not os.path.exists(fa_path) or not os.path.exists(meta_path):
        return False
    try:
        with open(meta_path, "r") as json_file:
            data = json.load(json_file)
    except:
        return False
    if not set(SOURCE_METADATA_FILE_KEYS).issubset(set(data.keys())):
        return False
    return True


def make_collection_source_dir(
        root_dir: str,
        collection_source_dir: str,
        env: str,
        collection: str,
        release_ver: str
) -> str:
    """
    Helper function that creates a collection & source_version and link in data
    to that collection from the overall source data dir.
    """
    csd = os.path.join(root_dir, collection_source_dir, env, collection, release_ver)
    os.makedirs(csd, exist_ok=True)
    return csd


def create_softlinks_in_csd(csd: str, work_dir: str, genome_ids: list[str], taxonomy_files: list[str] = []) -> None:
    """
    Create softlinks in the collection source dir to the genome files in the work dir.
    """
    for genome_id in genome_ids:
        genome_dir = os.path.join(work_dir, genome_id)
        csd_genome_dir = os.path.join(csd, genome_id)
        create_softlink_between_dirs(csd_genome_dir, genome_dir)

    for taxonomy_file in taxonomy_files:
        csd_file = os.path.join(csd, taxonomy_file)
        sd_file = os.path.join(work_dir, taxonomy_file)
        create_softlink_between_files(csd_file, sd_file)

    print(f"Genome files in {csd} \nnow link to {work_dir}")


def create_softlink_between_dirs(csd_dir, sd_dir):
    """
    Creates a softlink between two directories.
    """
    if os.path.exists(csd_dir):
        if (
                os.path.isdir(csd_dir)
                and os.path.islink(csd_dir)
                and os.readlink(csd_dir) == sd_dir
        ):
            return
        raise ValueError(
            f"{csd_dir} already exists and does not link to {sd_dir} as expected"
        )
    os.symlink(sd_dir, csd_dir, target_is_directory=True)


def create_softlink_between_files(csd_file, sd_file):
    """
    Creates a softlink between two files.
    """
    if os.path.exists(csd_file):
        if (os.path.islink(csd_file) and os.readlink(csd_file) == sd_file):
            return
        raise ValueError(
            f"{csd_file} already exists and does not link to {sd_file} as expected"
        )
    os.symlink(sd_file, csd_file)


def get_ip():
    """
    Get current ip address.
    """
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    return ip


def find_free_port():
    """
    Dynamically find a free port.
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def create_global_fatal_dict_doc(tool, error_message, stacktrace=None):
    doc = {FATAL_TOOL: tool,
           FATAL_ERROR: error_message,
           FATAL_STACKTRACE: stacktrace}
    return doc


class ExplicitDefaultsHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    def _get_help_string(self, action):
        if action.default is None or action.default is False:
            return action.help
        return super()._get_help_string(action)
