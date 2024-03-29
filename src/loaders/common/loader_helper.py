import argparse
import configparser
import fcntl
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
)
from src.common.storage.db_doc_conversions import (
    collection_data_id_key,
    collection_load_version_key,
)
import src.loaders.common.loader_common_names as loader_common_names


"""
This module contains helper functions used for loaders (e.g. compute_genome_attribs, gtdb_genome_attribs_loader, etc.)
"""

NONE_STR = ['N/A', 'NA', 'None', 'none', 'null', 'Null', 'NULL', '']


def _convert_to_iso8601(date_string: str) -> str:
    # Convert a date string to ISO 8601 format
    formats_to_try = ["%Y/%m/%d",
                      "%Y-%m-%d",
                      "%m/%d/%y", ]  # Add more formats as needed
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


def _convert_values_to_type(
        docs: list[dict],
        key: str,
        col_type: ColumnType,
        ignore_missing: bool = False,
        no_cast: bool = False):
    # Convert the values of a column to the specified type
    values = []
    for doc in docs:
        if key not in doc:
            if ignore_missing:
                continue
            raise ValueError(f'Unable to find key: {key} in {doc}')
        try:
            value = doc[key]
            value = None if value in NONE_STR else value
            if value is None:
                doc[key] = value
                continue
            if not no_cast:
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
        except ValueError as e:
            raise ValueError(f'Unable to convert value: {key} from {doc} to type: {col_type}') from e

    return values


def process_columnar_meta(
        docs: list[dict],
        kbase_collection: str,
        load_ver: str,
        product_id: str,
        ignore_missing: bool = False
):
    """
    Process the columnar metadata for the genome attributes.

    :param docs: the list of documents
    :param kbase_collection: the KBase collection name
    :param load_ver: the load version
    :param product_id: the product id
    :param ignore_missing: whether to ignore absent keys in the documents from the column spec file (default: False)
    """

    spec = load_spec(product_id, kbase_collection)
    columns = list()
    for col_spec in spec.columns:
        values = _convert_values_to_type(docs,
                                         col_spec.key,
                                         col_spec.type,
                                         ignore_missing=ignore_missing,
                                         no_cast=col_spec.no_cast)
        min_value, max_value, enum_values = None, None, None
        if col_spec.type in [ColumnType.INT, ColumnType.FLOAT, ColumnType.DATE]:
            if values:
                min_value, max_value = min(values), max(values)
            else:
                # set min_value and max_value to None if all values from the column are None
                min_value = max_value = None

        elif col_spec.type == ColumnType.ENUM:
            enum_values = list(set(values))
            enum_values.sort()

        attri_column = {
            'min_value': min_value,
            'max_value': max_value,
            'enum_values': enum_values,
            **col_spec.model_dump()
        }

        columns.append(attri_column)

    meta_doc = {'columns': columns, 'count': len(docs)}
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
    import_dir = Path(root_dir, loader_common_names.IMPORT_DIR, env, kbase_collection, load_ver)
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
        token = os.environ.get(loader_common_names.KB_AUTH_TOKEN)
    if not token:
        raise ValueError(
            f"Need to provide a token in the {loader_common_names.KB_AUTH_TOKEN} "
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
    os.environ["DOCKER_HOST"] = loader_common_names.DOCKER_HOST.format(uid)
    return proc


def _get_containers_config(conf_path: str):
    """Get containers.conf file at home directory."""
    config = configparser.ConfigParser()
    config.read(conf_path)
    return config


def is_config_modification_required():
    """check if the config requires modification."""
    conf_path = os.path.expanduser(loader_common_names.CONTAINERS_CONF_PATH)
    config = _get_containers_config(conf_path)
    if not config.has_section("containers"):
        return True
    for key, val in loader_common_names.CONTAINERS_CONF_PARAMS.items():
        if config.get("containers", key, fallback=None) != val:
            return True
    return False


def setup_callback_server_logs():
    """Set up containers.conf file for the callback server logs."""
    conf_path = os.path.expanduser(loader_common_names.CONTAINERS_CONF_PATH)
    with open(conf_path, "w") as writer:
        try:
            fcntl.flock(writer.fileno(), fcntl.LOCK_EX)
            config = _get_containers_config(conf_path)

            if not config.has_section("containers"):
                config.add_section("containers")

            for key, val in loader_common_names.CONTAINERS_CONF_PARAMS.items():
                config.set("containers", key, val)

            config.write(writer)
            print(f"containers.conf is modified and saved to path: {conf_path}")
        finally:
            fcntl.flock(writer.fileno(), fcntl.LOCK_UN)


def is_upa_info_complete(upa_dir: str):
    """
    Check whether an UPA needs to be downloaded or not by loading the metadata file.
    Make sure it has all the right keys.
    """
    upa = os.path.basename(upa_dir)

    # check if the FASTA file exists
    if not any(os.path.exists(os.path.join(upa_dir, upa + ext)) for ext in loader_common_names.FASTA_FILE_EXT):
        return False

    # check if the metadata file exists and the content is valid
    meta_path = get_meta_file_path(os.path.dirname(upa_dir), upa)
    if not os.path.exists(meta_path):
        return False
    try:
        with open(meta_path, "r") as json_file:
            data = json.load(json_file)
    except:
        return False
    if not set(loader_common_names.SOURCE_METADATA_FILE_KEYS).issubset(set(data.keys())):
        return False
    return True


def make_job_dir(root_dir, username):
    """Helper function that creates a job_dir for a user under root directory."""
    job_dir = os.path.join(root_dir, loader_common_names.SDK_JOB_DIR, username, uuid.uuid4().hex)
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
    output_dir = os.path.join(root_dir,
                              loader_common_names.SOURCE_DATA_DIR,
                              loader_common_names.WS,
                              env,
                              str(workspace_id))
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def make_collection_source_dir(root_dir: str, env: str, collection: str, source_ver: str) -> str:
    """
    Helper function that creates a collection & source_version and link in data
    to that collection from the overall source data dir.
    """
    collection_source_dir = os.path.join(root_dir,
                                         loader_common_names.COLLECTION_SOURCE_DIR,
                                         env,
                                         collection,
                                         source_ver)
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
                and os.readlink(new_dir) == str(target_dir)
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
        if os.path.islink(new_file) and os.readlink(new_file) == str(target_file):
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


def list_objects(wsid, ws, object_type, include_metadata=False, batch_size=10000):
    """
    List all objects information given a workspace ID.

    Args:
        wsid (int): Target workspace addressed by the permanent ID
        ws (Workspace): Workspace client
        object_type (str): Type of the objects to be listed
        include_metadata (boolean): Whether to include the user provided metadata in the returned object_info
        batch_size (int): Number of objects to process in each batch

    Returns:
        list: a list of objects on the target workspace

    """
    if batch_size > 10000:
        raise ValueError("Maximum value for listing workspace objects is 10000")

    maxObjectID = ws.get_workspace_info({"id": wsid})[4]
    batch_input = [
        [idx + 1, idx + batch_size] for idx in range(0, maxObjectID, batch_size)
    ]
    objs = [
        ws.list_objects(
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
    doc = {loader_common_names.FATAL_TOOL: tool,
           loader_common_names.FATAL_ERROR: error_message,
           loader_common_names.FATAL_STACKTRACE: stacktrace}
    return doc


class ExplicitDefaultsHelpFormatter(argparse.ArgumentDefaultsHelpFormatter):
    def _get_help_string(self, action):
        if action.default is None or action.default is False:
            return action.help
        return super()._get_help_string(action)


def generate_import_dir_meta(
        assembly_obj_info: list[Any],
        genome_obj_info: list[Any]
) -> dict[str, Any]:
    """
    Generate metadata by processing Assembly and Genome object information, tailored for the parser

    assembly_obj_info - Workspace Assembly object info
    genome_obj_info - Workspace Genome object info

    "upa", "name", "type", and "timestamp" info will be extracted from assembly object info and saved as a dict.
    {
        "upa": <assembly object upa>
        "name": <copy object name from assembly object info>
        "type": <copy object type from assembly object info>
        "timestamp": <copy timestamp from assembly object info>
        "genome_upa": <genome object upa>
        "assembly_obj_info": <copy assembly object info>
        "genome_obj_info": <copy genome object info>
    }
    """
    res_dict = {loader_common_names.FLD_KB_OBJ_UPA: "{6}/{0}/{4}".format(*assembly_obj_info),
                loader_common_names.FLD_KB_OBJ_NAME: assembly_obj_info[1],
                loader_common_names.FLD_KB_OBJ_TYPE: assembly_obj_info[2],
                loader_common_names.FLD_KB_OBJ_TIMESTAMP: assembly_obj_info[3],
                loader_common_names.FLD_KB_OBJ_GENOME_UPA: "{6}/{0}/{4}".format(*genome_obj_info),
                loader_common_names.ASSEMBLY_OBJ_INFO_KEY: assembly_obj_info,
                loader_common_names.GENOME_OBJ_INFO_KEY: genome_obj_info}

    return res_dict


def dump_json_to_file(json_file_path: str | Path, json_data: dict[str, Any] | list[Any]) -> None:
    """
    Dump json data to a file.

    json_file_path - the path to the result json file
    json_data - the json data to be dumped
    """
    with open(json_file_path, "w", encoding="utf8") as json_file:
        json.dump(json_data, json_file, indent=2)


def get_meta_file_path(
        source_dir: Path | str,
        structured_upa: str
) -> Path:
    """
    Get the metadata file path for a specific workspace id under sourcedata/WS/<env>.

    source_dir - The directory for a specific workspace id under sourcedata/WS/<env>
    structured_upa - The UPA of a workspace object in the format of "<wsid>_<objid>_<ver>"
    """

    metafile = Path(source_dir) / structured_upa / f"{structured_upa}.meta"
    # Ensure the directory structure exists, create if not
    metafile.parent.mkdir(parents=True, exist_ok=True)

    return metafile


def create_meta_file(
        source_dir: Path | str,
        structured_upa: str,
        assembly_obj_info: list[Any],
        genome_obj_info: list[Any],
) -> Path:
    """
    Generates a metadata file for a workspace object and saves it in the associated sourcedata directory.

    source_dir - The directory for a specific workspace id under sourcedata/WS/<env>.
    structured_upa - The UPA of a workspace object in the format of "<wsid>_<objid>_<ver>"
    assembly_obj_info - Workspace Assembly object info
    genome_obj_info - Workspace Genome object info
    """
    metafile = get_meta_file_path(source_dir, structured_upa)

    dump_json_to_file(metafile, generate_import_dir_meta(assembly_obj_info, genome_obj_info))

    return metafile