import argparse
import itertools
import json
import os
import socket
import stat
import subprocess
import time
import uuid
from collections import defaultdict
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Any

import jsonlines

import src.common.storage.collection_and_field_names as names
from src.common.collection_column_specs.load_specs import load_spec
from src.common.product_models.columnar_attribs_common_models import (
    ColumnType,
    AttributesColumn,
    ColumnarAttributesMeta,
)
from src.common.product_models.heatmap_common_models import (
    FIELD_HEATMAP_ROW_CELLS,
    FIELD_HEATMAP_COL_ID,
    FIELD_HEATMAP_CELL_ID,
    FIELD_HEATMAP_CELL_VALUE,
)
from src.common.storage.db_doc_conversions import (
    collection_data_id_key,
    collection_load_version_key,
)
from src.loaders.common.loader_common_names import (
    COLLECTION_SOURCE_DIR,
    DOCKER_HOST,
    FATAL_ERROR,
    FATAL_STACKTRACE,
    FATAL_TOOL,
    IMPORT_DIR,
    KB_AUTH_TOKEN,
    SDK_JOB_DIR,
    SOURCE_DATA_DIR,
    SOURCE_METADATA_FILE_KEYS,
    WS,
)

"""
This module contains helper functions used for loaders (e.g. compute_genome_attribs, gtdb_genome_attribs_loader, etc.)
"""

NONE_STR = ['N/A', 'NA', 'None', 'none', 'null', 'Null', 'NULL', '']

HEATMAP_COL_PREFIX = "col"
HEATMAP_COL_SEPARATOR = '_'


def _convert_to_iso8601(date_string: str) -> str:
    # Convert a date string to ISO 8601 format
    formats_to_try = ["%Y/%m/%d", "%Y-%m-%d"]  # Add more formats as needed
    # The current code always leaves the date in day precision with no time zone information as that's
    # all that's available from the current data.
    # If higher precision dates are encountered in the future the code should be adapted to
    # keep as much precision as possible (but don't add precision that isn't there), including
    # timezone info if available.
    for date_format in formats_to_try:
        try:
            parsed_date = datetime.strptime(date_string, date_format)
            iso8601_date = parsed_date.date().isoformat()
            return iso8601_date
        except ValueError:
            continue

    raise ValueError("Unrecognized date format")


def _convert_values_to_type(docs: list[dict], key: str, col_type: ColumnType):
    # Convert the values of a column to the specified type
    values = []
    for doc in docs:
        try:
            value = doc[key]
            value = None if value in NONE_STR else value
            if value is None:
                doc[key] = value
                continue

            if col_type == ColumnType.INT:
                doc[key] = int(value)
            elif col_type == ColumnType.FLOAT:
                doc[key] = float(value)
            elif col_type == ColumnType.STRING:
                doc[key] = str(value)
            elif col_type == ColumnType.DATE:
                doc[key] = _convert_to_iso8601(value)
            else:
                raise ValueError(f'casting not implemented for {col_type}')

            values.append(doc[key])
        except KeyError as e:
            raise ValueError(f'Unable to find key: {key} in {doc}') from e
        except ValueError as e:
            raise ValueError(f'Unable to convert value: {key} from {doc} to type: {col_type}') from e

    return values


def process_columnar_meta(
        docs: list[dict],
        kbase_collection: str,
        load_ver: str,
):
    """
    Process the columnar metadata for the genome attributes.

    :param docs: the list of documents
    :param kbase_collection: the KBase collection name
    :param load_ver: the load version
    """

    spec = load_spec(names.GENOME_ATTRIBS_PRODUCT_ID, kbase_collection)
    columns = list()
    for col_spec in spec.columns:
        key, col_type = col_spec.key, col_spec.type
        values = _convert_values_to_type(docs, key, col_type)
        min_value, max_value, enum_values = None, None, None
        if col_type in [ColumnType.INT, ColumnType.FLOAT, ColumnType.DATE]:
            if values:
                min_value, max_value = min(values), max(values)
            else:
                # set min_value and max_value to None if all values from the column are None
                min_value = max_value = None

        elif col_type == ColumnType.ENUM:
            enum_values = list(set(values))
            enum_values.sort()

        attri_column = AttributesColumn(
            **col_spec.model_dump(),
            min_value=min_value,
            max_value=max_value,
            enum_values=enum_values
        )
        columns.append(attri_column)

    columnar_attri_meta = ColumnarAttributesMeta(columns=columns, count=len(docs))

    meta_doc = columnar_attri_meta.model_dump()
    meta_doc.update({
        names.FLD_ARANGO_KEY: collection_load_version_key(kbase_collection, load_ver),
        names.FLD_COLLECTION_ID: kbase_collection,
        names.FLD_LOAD_VERSION: load_ver
    })

    return docs, meta_doc


def convert_to_json(docs, outfile):
    """
    Writes list of dictionaries to a file-like-object in JSON Lines format.

    Args:
        docs: list of dictionaries
        outfile: an argparse File (e.g. argparse.FileType('w')) object
    """

    with jsonlines.Writer(outfile) as writer:
        writer.write_all(docs)


def create_import_dir(
        root_dir: str,
        env: str,
        kbase_collection: str,
        load_ver: str,
) -> Path:
    """
    Create the import directory for the given environment.
    """
    import_dir = Path(root_dir, IMPORT_DIR, env, kbase_collection, load_ver)
    os.makedirs(import_dir, exist_ok=True)

    return import_dir


def create_import_files(
        root_dir: str,
        env: str,
        kbase_collection: str,
        load_ver: str,
        file_name: str,
        docs: list[dict[str, Any]]):
    """
    Create and save the data documents as JSONLines file to the import directory.
    """
    import_dir = create_import_dir(root_dir, env, kbase_collection, load_ver)

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
    Start podman service. Used by workspace_downloader.py and workspace_uploader.py scripts.

    uid - the integer unix user ID of the user running the service.
    """
    # TODO find out the right way to check if a podman service is running
    command = ["podman", "system", "service", "-t", "0"]
    proc = subprocess.Popen(command)
    time.sleep(1)
    return_code = proc.poll()
    if return_code:
        raise ValueError(f"The command {command} failed with return code {return_code}. "
                         f"Podman service failed to start")
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


def make_job_dir(root_dir, username):
    """Helper function that creates a job_dir for a user under root directory."""
    job_dir = os.path.join(root_dir, SDK_JOB_DIR, username, uuid.uuid4().hex)
    os.makedirs(job_dir, exist_ok=True)
    # only user can read, write, or execute
    os.chmod(job_dir, stat.S_IRWXU)
    return job_dir


def make_job_data_dir(job_dir):
    """
    Helper function that creates a temporary directory for sharing files between the host, callback server, and container.
    
    SDK modules (like AssemblyUtil) have the shared directory mounted in the container at `/kb/module/work`. The
    scratch directory provided to the SDK module `*Impl.py` code is `/kb/module/work/tmp`. The SDK code is expected
    to read and write shared files there.
    
    The callback server mounts `<job_dir>/workdir` as the host shared directory into the SDK module.
    
    `<job_dir>` is also mounted into the callback server and it writes job information (e.g. the token and job configuration) 
    into `<job_dir>/workdir`
    """
    data_dir = os.path.join(job_dir, "workdir/tmp")
    os.makedirs(data_dir)
    return data_dir


def make_sourcedata_ws_dir(root_dir, env, workspace_id):
    """Helper function that creates a output directory for a specific workspace id under root directory."""
    output_dir = os.path.join(root_dir, SOURCE_DATA_DIR, WS, env, str(workspace_id))
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def make_collection_source_dir(root_dir: str, env: str, collection: str, source_ver: str) -> str:
    """
    Helper function that creates a collection & source_version and link in data
    to that collection from the overall source data dir.
    """
    collection_source_dir = os.path.join(root_dir, COLLECTION_SOURCE_DIR, env, collection, source_ver)
    os.makedirs(collection_source_dir, exist_ok=True)
    return collection_source_dir


def create_softlinks_in_collection_source_dir(
        collection_source_dir: str,
        work_dir: str,
        genome_ids: list[str],
        taxonomy_files: list[str] = None
) -> None:
    """
    Create softlinks in the collection source dir to the genome files in the work dir.
    """
    if not taxonomy_files:
        taxonomy_files = []

    for genome_id in genome_ids:
        target_dir = os.path.join(work_dir, genome_id)
        new_dir = os.path.join(collection_source_dir, genome_id)
        create_softlink_between_dirs(new_dir, target_dir)

    for taxonomy_file in taxonomy_files:
        new_file = os.path.join(collection_source_dir, taxonomy_file)
        target_file = os.path.join(work_dir, taxonomy_file)
        create_softlink_between_files(new_file, target_file)

    print(f"Genome files in {collection_source_dir} \nnow link to {work_dir}")


def create_softlink_between_dirs(new_dir, target_dir):
    """
    Creates a softlink from new_dir to the contents of target_dir.
    """
    if os.path.exists(new_dir):
        if (
                os.path.isdir(new_dir)
                and os.path.islink(new_dir)
                and os.readlink(new_dir) == target_dir
        ):
            return
        raise ValueError(
            f"{new_dir} already exists and does not link to {target_dir} as expected"
        )
    os.symlink(target_dir, new_dir, target_is_directory=True)


def create_softlink_between_files(new_file, target_file):
    """
    Creates a softlink from new_file to the contents of target_file.
    """
    if os.path.exists(new_file):
        if (os.path.islink(new_file) and os.readlink(new_file) == target_file):
            return
        raise ValueError(
            f"{new_file} already exists and does not link to {target_file} as expected"
        )
    os.symlink(target_file, new_file)


def create_hardlink_between_files(new_file, target_file):
    """
    Creates a hardlink from new_file to the contents of target_file.
    """
    if os.path.exists(new_file):
        if os.path.samefile(target_file, new_file):
            return
        raise ValueError(
            f"{new_file} already exists and does not link to {target_file} as expected"
        )
    os.link(target_file, new_file)


def list_objects(wsid, conf, object_type, include_metadata=False, batch_size=10000):
    """
    List all objects information given a workspace ID.
    """
    if batch_size > 10000:
        raise ValueError("Maximum value for listing workspace objects is 10000")

    maxObjectID = conf.ws.get_workspace_info({"id": wsid})[4]
    batch_input = [
        [idx + 1, idx + batch_size] for idx in range(0, maxObjectID, batch_size)
    ]
    objs = [
        conf.ws.list_objects(
            _list_objects_params(wsid, min_id, max_id, object_type, include_metadata)
        )
        for min_id, max_id in batch_input
    ]
    res_objs = list(itertools.chain.from_iterable(objs))
    return res_objs


def _list_objects_params(wsid, min_id, max_id, type_str, include_metadata):
    """Helper function that creates params needed for list_objects function."""
    params = {
        "ids": [wsid],
        "minObjectID": min_id,
        "maxObjectID": max_id,
        "type": type_str,
        "includeMetadata": int(include_metadata),
    }
    return params


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


def transform_heatmap_row_cells(data: dict[str, Any]):
    """
    Transform, in place, the cells structure in a heatmap row to a new structure.

    The new structure is a set of keys and values where the keys are constructed from the old structure.
    new structured key format: <HEATMAP_COL_PREFIX>_<col_id>_<FIELD_HEATMAP_CELL_ID|FIELD_HEATMAP_CELL_VALUEl>

    e.g.

    The old structure:
    "cells": [
        {
            "cell_id": "cell_0",
            "col_id": "0",
            "val": 0.0
        },
        {
            "cell_id": "cell_1",
            "col_id": "1",
            "val": 1.0
        }
    ]

    The new structure:
    "col_0_cell_id": "cell_0",
    "col_0_val": 0.0,
    "col_1_cell_id": "cell_1",
    "col_1_val": 1.0

    """

    # Iterate over the 'cells' structure and remove them from the data structure while constructing the new structure
    for cell in data.pop(FIELD_HEATMAP_ROW_CELLS):
        col_id = cell.get(FIELD_HEATMAP_COL_ID)

        # Construct keys and values for the new structure
        cell_id_key = f"{HEATMAP_COL_PREFIX}{HEATMAP_COL_SEPARATOR}{col_id}{HEATMAP_COL_SEPARATOR}{FIELD_HEATMAP_CELL_ID}"
        cell_val_key = f"{HEATMAP_COL_PREFIX}{HEATMAP_COL_SEPARATOR}{col_id}{HEATMAP_COL_SEPARATOR}{FIELD_HEATMAP_CELL_VALUE}"

        # Add the new keys and values to the data structure
        data[cell_id_key] = cell.get(FIELD_HEATMAP_CELL_ID)
        data[cell_val_key] = cell.get(FIELD_HEATMAP_CELL_VALUE)


