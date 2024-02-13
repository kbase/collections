"""
usage: workspace_uploader.py [-h] --workspace_id WORKSPACE_ID [--kbase_collection KBASE_COLLECTION] [--source_ver SOURCE_VER]
                             [--root_dir ROOT_DIR] [--load_id LOAD_ID] [--token_filepath TOKEN_FILEPATH] [--env {CI,NEXT,APPDEV,PROD}]
                             [--create_assembly_only] [--upload_file_ext UPLOAD_FILE_EXT [UPLOAD_FILE_EXT ...]] [--batch_size BATCH_SIZE]
                             [--cbs_max_tasks CBS_MAX_TASKS] [--au_service_ver AU_SERVICE_VER] [--keep_job_dir] [--as_catalog_admin]

PROTOTYPE - Upload files to the workspace service (WSS). Note that the uploader determines whether a genome is already uploaded in
one of two ways. First it consults the *.yaml files in each genomes directory; if that file shows the genome has been uploaded it skips it
regardless of the current state of the workspace. Second, it checks that the most recent version of the genome object in the workspace, if it
exists, was part of the current load ID (see the load ID parameter description below). If so, the genome is skipped.

options:
  -h, --help            show this help message and exit

required named arguments:
  --workspace_id WORKSPACE_ID
                        Target workspace addressed by the permanent ID
  --kbase_collection KBASE_COLLECTION
                        The name of the collection being processed
                        Specifies where the files to be uploaded exist (in the default NONE environment)
                        and the name of the collection to be created in the specific KBase environment in the 'collectionsource' directory
  --source_ver SOURCE_VER
                        The source version of the collection being processed
                        Specifies where the files to be uploaded exist (in the default NONE environment)
                        and the source version of the collection to be created in the specific KBase environment in the 'collectionsource' directory

optional arguments:
  --root_dir ROOT_DIR   Root directory for the collections project. (default: /global/cfs/cdirs/kbase/collections)
  --load_id LOAD_ID     The load id of the objects being uploaded to a workspace. If not provided, a random load_id will be generated. For a
                        particular load, any restarts / resumes of the load should use the same load ID to prevent reuploading the same data.
                        A new load ID will make new versions of all the objects from the prior upload.
  --token_filepath TOKEN_FILEPATH
                        A file path that stores a KBase token appropriate for the KBase environment.
                        If not provided, the token must be provided in the `KB_AUTH_TOKEN` environment variable.
  --env {CI,NEXT,APPDEV,PROD}
                        KBase environment (default: PROD)
  --create_assembly_only
                        Create only assembly object. If not set create Genome object using genbank files by default.
  --upload_file_ext UPLOAD_FILE_EXT [UPLOAD_FILE_EXT ...]
                        Upload only files that match given extensions. If not provided, uses the appropriate default extension
                        depending on the create_assembly_only flag
  --batch_size BATCH_SIZE
                        Number of files to upload per batch (default: 2500)
  --cbs_max_tasks CBS_MAX_TASKS
                        The maximum number of subtasks for the callback server (default: 20)
  --au_service_ver AU_SERVICE_VER
                        The service version of AssemblyUtil client('dev', 'beta', 'release', or a git commit) (default: release)
  --keep_job_dir        Keep SDK job directory after upload task is completed
  --as_catalog_admin    True means the provided user token has catalog admin privileges and will be used to retrieve secure SDK app
                        parameters from the catalog. If false, the default, SDK apps run as part of this application will not have access to
                        catalog secure parameters.

e.g.
PYTHONPATH=. python src/loaders/workspace_uploader/workspace_uploader.py --workspace_id 69046 --kbase_collection GTDB --source_ver 207 --env CI --keep_job_dir

NOTE:
NERSC file structure for WS:

/global/cfs/cdirs/kbase/collections/sourcedata/ -> WS -> ENV -> workspace ID -> UPA -> .fna.gz file

e.g.
/global/cfs/cdirs/kbase/collections/sourcedata/WS -> CI -> 69046 -> 69046_58_1 -> 69046_58_1.fna.gz
                                                                    69046_60_1 -> 69046_60_1.fna.gz

The data will be linked to the collections source directory:
e.g. /global/cfs/cdirs/kbase/collections/collectionssource/ -> ENV -> kbase_collection -> source_ver -> UPA -> .fna.gz file
"""
import argparse
import click
import os
import shutil
import time
import uuid
from collections import namedtuple
from datetime import datetime
from pathlib import Path
from typing import Generator, Any

import yaml

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_common_names, loader_helper
from src.loaders.common.callback_server_wrapper import Conf

# setup KB_AUTH_TOKEN as env or provide a token_filepath in --token_filepath
# export KB_AUTH_TOKEN="your-kb-auth-token"

_UPLOAD_ASSEMBLY_FILE_EXT = ["genomic.fna.gz"]
_UPLOAD_GENOME_FILE_EXT = ["genomic.gbff.gz"]
_JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER = "/kb/module/work/tmp"
_UPLOADED_YAML = "uploaded.yaml"
_WS_MAX_BATCH_SIZE = 10000

_WSObjTuple = namedtuple(
    "WSObjTuple",
    ["obj_name", "host_file_dir", "container_internal_file_dir"],
)

# keys for the uploaded.yaml file
_KEY_ASSEMBLY_UPA = "assembly_upa"
_KEY_ASSEMBLY_FILENAME = "assembly_filename"
_KEY_GENOME_UPA = "genome_upa"
_KEY_GENOME_FILENAME = "genome_filename"


def _get_parser():
    parser = argparse.ArgumentParser(
        description="PROTOTYPE - Upload files to the workspace service (WSS).\n\n"
        "Note that the uploader determines whether a genome is already uploaded in one of two ways. "
        "First it consults the *.yaml files in each genomes directory; if that file shows the genome "
        "has been uploaded it skips it regardless of the current state of the workspace. "
        "Second, it checks that the most recent version of the genome object in the workspace, "
        "if it exists, was part of the current load ID (see the load ID parameter description below). "
        "If so, the genome is skipped.",
        formatter_class=loader_helper.ExplicitDefaultsHelpFormatter,
    )

    required = parser.add_argument_group("required named arguments")
    optional = parser.add_argument_group("optional arguments")

    # Required flag argument
    required.add_argument(
        "--workspace_id",
        required=True,
        type=int,
        help="Target workspace addressed by the permanent ID",
    )
    required.add_argument(
        f"--{loader_common_names.KBASE_COLLECTION_ARG_NAME}",
        type=str,
        help="The name of the collection being processed. "
        "Specifies where the files to be uploaded exist (in the default NONE environment) "
        "and the name of the collection to be created in the specific KBase environment in the 'collectionsource' directory.",
    )
    required.add_argument(
        f"--{loader_common_names.SOURCE_VER_ARG_NAME}",
        type=str,
        help="The source version of the collection being processed. "
        "Specifies where the files to be uploaded exist (in the default NONE environment) "
        "and the source version of the collection to be created in the specific KBase environment in the 'collectionsource' directory.",
    )

    # Optional argument
    optional.add_argument(
        f"--{loader_common_names.ROOT_DIR_ARG_NAME}",
        type=str,
        default=loader_common_names.ROOT_DIR,
        help=loader_common_names.ROOT_DIR_DESCR,
    )
    optional.add_argument(
        "--load_id",
        type=str,
        help="The load id of the objects being uploaded to a workspace. "
        "If not provided, a random load_id will be generated. "
        "For a particular load, any restarts / resumes of the load should use the same load ID to prevent reuploading the same data. "
        "A new load ID will make new versions of all the objects from the prior upload.",
    )
    optional.add_argument(
        "--token_filepath",
        type=str,
        help="A file path that stores a KBase token appropriate for the KBase environment. "
        "If not provided, the token must be provided in the `KB_AUTH_TOKEN` environment variable.",
    )
    optional.add_argument(
        "--env",
        type=str,
        choices=loader_common_names.KB_ENV,
        default="PROD",
        help="KBase environment",
    )
    optional.add_argument(
        "--create_assembly_only",
        action="store_true",
        help="Create only assembly object. If not set create Genome object using genbank files by default.",
    )
    optional.add_argument(
        "--upload_file_ext",
        type=str,
        nargs="+",
        help="Upload only files that match given extensions. If not provided, uses the appropriate default extension "
             "depending on the create_assembly_only flag",
    )
    optional.add_argument(
        "--batch_size",
        type=int,
        default=2500,
        help="Number of files to upload per batch",
    )
    optional.add_argument(
        "--cbs_max_tasks",
        type=int,
        default=20,
        help="The maximum number of subtasks for the callback server",
    )
    optional.add_argument(
        "--au_service_ver",
        type=str,
        default="release",
        help="The service version of AssemblyUtil client"
        "('dev', 'beta', 'release', or a git commit)",
    )
    optional.add_argument(
        "--keep_job_dir",
        action="store_true",
        help="Keep SDK job directory after upload task is completed",
    )
    optional.add_argument(
        "--as_catalog_admin",
        action="store_true",
        help="True means the provided user token has catalog admin privileges and will "
        "be used to retrieve secure SDK app parameters from the catalog. If false, the default, "
        "SDK apps run as part of this application will not have access to catalog secure parameters.",
    )
    return parser


def _get_default_file_ext(create_assembly_only: bool) -> list[str]:
    return _UPLOAD_ASSEMBLY_FILE_EXT if create_assembly_only else _UPLOAD_GENOME_FILE_EXT


def _get_yaml_file_path(obj_dir: str) -> str:
    """
    Get the uploaded.yaml file path from collections source directory.
    """
    file_path = os.path.join(obj_dir, _UPLOADED_YAML)
    Path(file_path).touch(exist_ok=True)
    return file_path


def _get_source_file(obj_dir: str, obj_file: str) -> str:
    """
    Get the sourcedata file path from the WS object directory.
    """
    if not os.path.islink(obj_dir):
        raise ValueError(f"{obj_dir} is not a symlink")
    src_file = os.path.join(os.readlink(obj_dir), obj_file)
    return src_file


def _upload_assemblies_to_workspace(
    asu: AssemblyUtil,
    workspace_id: int,
    load_id: int,
    ws_obj_tuples: list[_WSObjTuple],
) -> tuple[str, ...]:
    """
    Upload assembly files to the target workspace in batch. The bulk method fails
    and an error will be thrown if any of the assembly files in batch fails to upload.
    Returns the list of workspace UPAs for the created objects in the same order as
    `ws_obj_tuples`.
    """
    inputs = [
        {
            "file": obj_tuple.container_internal_file_dir,
            "assembly_name": obj_tuple.obj_name,
            "object_metadata": {"load_id": load_id},
        }
        for obj_tuple in ws_obj_tuples
    ]

    assembly_ref = asu.save_assemblies_from_fastas(
        {"workspace_id": workspace_id, "inputs": inputs}
    )

    upas = tuple(
        [
            result_dict["upa"].replace("/", "_")
            for result_dict in assembly_ref["results"]
        ]
    )
    return upas


def _read_upload_status_yaml_file(
    upload_env_key: str,
    workspace_id: int,
    load_id: str,
    obj_dir: str,
    create_assembly_only: bool = True,
) -> tuple[dict[str, dict[int, list[str]]], bool]:
    """
    Get metadata and upload status of a WS object from the uploaded.yaml file.

    Structure of result yaml file:
    <env>:
        <workspace_id>:
            <load_id>:
                assembly_upa: <assembly_upa>
                assembly_filename: <assembly_filename>
                genome_upa: <genome_upa> (can be None)
                genome_filename: <genome_filename> (can be None)
    """

    if upload_env_key not in loader_common_names.KB_ENV:
        raise ValueError(
            f"Currently only support these {loader_common_names.KB_ENV} envs for upload"
        )

    file_path = _get_yaml_file_path(obj_dir)

    with open(file_path, "r") as file:
        data = yaml.safe_load(file) or dict()

    workspace_dict = data.setdefault(upload_env_key, {}).setdefault(workspace_id, {})

    uploaded = load_id in workspace_dict

    # In the event of genome upload and an assembly with the identical load_id has already been uploaded.
    if not create_assembly_only:
        uploaded = uploaded and workspace_dict[load_id].get(_KEY_GENOME_UPA)

    return data, uploaded


def _update_upload_status_yaml_file(
    upload_env_key: str,
    workspace_id: int,
    load_id: str,
    assembly_upa: str,
    assembly_tuple: _WSObjTuple,
    genome_upa: str = None,
    genome_tuple: _WSObjTuple = None,
) -> None:
    """
    Update the uploaded.yaml file in target genome_dir with newly uploaded WS object names and upa info.
    """
    obj_tuple = genome_tuple if genome_tuple else assembly_tuple

    data, uploaded = _read_upload_status_yaml_file(
        upload_env_key,
        workspace_id,
        load_id,
        obj_tuple.host_file_dir,
    )

    if uploaded:
        raise ValueError(
            f"Object {obj_tuple.obj_name} already exists in workspace {workspace_id}"
        )

    data[upload_env_key][workspace_id][load_id] = {
        _KEY_ASSEMBLY_UPA: assembly_upa,
        _KEY_ASSEMBLY_FILENAME: assembly_tuple.obj_name,
        _KEY_GENOME_UPA: genome_upa,
        _KEY_GENOME_FILENAME: genome_tuple.obj_name if genome_tuple else None,
    }

    file_path = _get_yaml_file_path(obj_tuple.host_file_dir)
    with open(file_path, "w") as file:
        yaml.dump(data, file)


def _fetch_objects_to_upload(
    upload_env_key: str,
    workspace_id: int,
    load_id: str,
    collection_source_dir: str,
    upload_file_ext: list[str],
) -> tuple[int, dict[str, str]]:
    count = 0
    wait_to_upload_objs = dict()

    obj_dirs = [
        os.path.join(collection_source_dir, d)
        for d in os.listdir(collection_source_dir)
        if os.path.isdir(os.path.join(collection_source_dir, d))
    ]

    for obj_dir in obj_dirs:
        obj_file_list = [
            f
            for f in os.listdir(obj_dir)
            if os.path.isfile(os.path.join(obj_dir, f))
            and f.endswith(tuple(upload_file_ext))
        ]

        # Assembly and Genome (from genbank) object uploaders both only requires one file
        # Modify or skip this check if a different use case requires multiple files.
        if len(obj_file_list) != 1:
            raise ValueError(
                f"One and only one object file that ends with {upload_file_ext} "
                f"must be present in {obj_dir} directory"
            )

        count += 1
        obj_name = obj_file_list[0]

        _, uploaded = _read_upload_status_yaml_file(
            upload_env_key,
            workspace_id,
            load_id,
            obj_dir,
        )

        if uploaded:
            print(
                f"Object {obj_name} already exists in "
                f"workspace {workspace_id} load {load_id}. Skipping."
            )
            continue

        wait_to_upload_objs[obj_name] = obj_dir

    return count, wait_to_upload_objs


def _query_workspace_with_load_id(
    ws: Workspace,
    workspace_id: int,
    load_id: str,
    obj_names: list[str],
) -> tuple[list[str], list[str]]:
    if len(obj_names) > _WS_MAX_BATCH_SIZE:
        raise ValueError(
            f"The effective max batch size must be <= {_WS_MAX_BATCH_SIZE}"
        )
    refs = [{"wsid": workspace_id, "name": name} for name in obj_names]
    res = ws.get_object_info3(
        {"objects": refs, "ignoreErrors": 1, "includeMetadata": 1}
    )
    uploaded_obj_names_batch = [
        info[1]
        for info in res["infos"]
        if info is not None and "load_id" in info[10] and info[10]["load_id"] == load_id
    ]
    uploaded_obj_upas_batch = [
        path[0].replace("/", "_") for path in res["paths"] if path is not None
    ]
    return uploaded_obj_names_batch, uploaded_obj_upas_batch


def _query_workspace_with_load_id_mass(
    ws: Workspace,
    workspace_id: int,
    load_id: str,
    obj_names: list[str],
    batch_size: int = _WS_MAX_BATCH_SIZE,
) -> tuple[list[str], list[str]]:
    uploaded_obj_names = []
    uploaded_obj_upas = []

    for idx in range(0, len(obj_names), batch_size):
        obj_names_batch, obj_upas_batch = _query_workspace_with_load_id(
            ws, workspace_id, load_id, obj_names[idx: idx + batch_size]
        )
        uploaded_obj_names.extend(obj_names_batch)
        uploaded_obj_upas.extend(obj_upas_batch)

    return uploaded_obj_names, uploaded_obj_upas


def _prepare_skd_job_dir_to_upload(
    conf: Conf, wait_to_upload_objs: dict[str, str]
) -> str:
    """
    Prepare SDK job directory to upload.
    """
    for obj_file, obj_dir in wait_to_upload_objs.items():
        src_file = _get_source_file(obj_dir, obj_file)
        dest_file = os.path.join(conf.job_data_dir, obj_file)
        loader_helper.create_hardlink_between_files(dest_file, src_file)

    return conf.job_data_dir


def _post_process(
    upload_env_key: str,
    workspace_id: int,
    load_id: str,
    upload_dir: str,
    output_dir: str,
    assembly_tuple: _WSObjTuple,
    assembly_upa: str,
    genome_tuple: _WSObjTuple = None,
    genome_upa: str = None,
    assembly_obj_info: list[Any] = None,
    genome_obj_info: list[Any] = None,
) -> None:
    """
    Update the uploaded.yaml file in the genome directory with the object name and upa info.

    If genome_tuple and genome_upa are provided, the function will also:
    Create a standard entry in sourcedata/workspace for each object.
    Hardlink to the original object file in sourcedata to avoid duplicating the file.
    Creates a softlink from new_dir in collectionssource to the contents of target_dir in sourcedata.
    """

    if (genome_tuple is None) != (genome_upa is None):
        raise ValueError(
            "Both genome_tuple and genome_upa must be provided if one of them is provided"
        )

    if genome_tuple and genome_upa:

        if not assembly_obj_info or not genome_obj_info:
            raise ValueError(
                "Both assembly_obj_info and genome_obj_info must be provided"
            )

        _process_genome_objects(
            upload_dir, output_dir, assembly_tuple, assembly_upa, assembly_obj_info, genome_obj_info)

    # Update the 'uploaded.yaml' file, serving as a marker to indicate the successful upload of the object.
    # Ensure that this operation is the final step in the post-processing workflow
    _update_upload_status_yaml_file(
        upload_env_key,
        workspace_id,
        load_id,
        assembly_upa,
        assembly_tuple,
        genome_upa=genome_upa,
        genome_tuple=genome_tuple,
    )


def _process_genome_objects(
        upload_dir: str,
        output_dir: str,
        assembly_tuple: _WSObjTuple,
        assembly_upa: str,
        assembly_obj_info: list[Any],
        genome_obj_info: list[Any],
) -> None:
    """
    Post process on successful genome uploads.
    """
    # Create a standard entry in sourcedata/workspace
    # hardlink to the original object file in sourcedata
    src_file = Path(_get_source_file(assembly_tuple.host_file_dir, assembly_tuple.obj_name))
    target_dir = os.path.join(output_dir, assembly_upa)
    os.makedirs(target_dir, exist_ok=True)

    suffixes = src_file.suffixes
    # TODO - handle extension other than .gz
    dest_suffix = suffixes[-1] if suffixes[-1] != ".gz" else "".join(suffixes[-2:])
    dest_file = os.path.join(target_dir, f"{assembly_upa}{dest_suffix}")
    loader_helper.create_hardlink_between_files(dest_file, src_file)

    # create metadata file used by parser
    meta = loader_helper.generate_import_dir_meta(assembly_obj_info, genome_obj_info)
    metafile = os.path.join(target_dir, f"{assembly_upa}.meta")
    loader_helper.dump_json_to_file(metafile, meta)

    # create a softlink from new_dir in collectionssource to the contents of target_dir in sourcedata
    new_dir = os.path.join(upload_dir, assembly_upa)
    loader_helper.create_softlink_between_dirs(new_dir, target_dir)


def _upload_assembly_files_in_parallel(
    asu: AssemblyUtil,
    ws: Workspace,
    upload_env_key: str,
    workspace_id: int,
    load_id: str,
    upload_dir: str,
    wait_to_upload_objs: dict[str, str],
    batch_size: int,
    output_dir: str,
) -> int:
    """
    Upload assembly files to the target workspace in parallel using multiprocessing.

    Parameters:
        asu: AssemblyUtil client
        ws: Workspace client
        upload_env_key: environment variable key in uploaded.yaml file
        workspace_id: target workspace id
        load_id: load id
        upload_dir: a directory in collectionssource that creates new directories linking to sourcedata
        wait_to_upload_objs: a dictionary that maps object(assembly) file name to object(assembly) directory
        batch_size: a number of files to upload per batch
        output_dir: a directory in sourcedata/workspace to store new assembly entries

    Returns:
        number of object(assembly) files have been successfully uploaded from wait_to_upload_assemblies
    """
    assembly_files_len = len(wait_to_upload_objs)
    print(f"Start uploading {assembly_files_len} assembly files\n")

    uploaded_count = 0
    uploaded_fail = False
    for obj_tuples in _gen(wait_to_upload_objs, batch_size):
        batch_upas = tuple()
        batch_uploaded_tuples = []

        try:
            batch_upas = _upload_assemblies_to_workspace(
                asu, workspace_id, load_id, obj_tuples
            )
            batch_uploaded_tuples = obj_tuples
        except Exception as e:
            print(e)
            uploaded_fail = True

            try:
                # figure out uploads that succeeded
                name2tuple = {
                    obj_tuple.obj_name: obj_tuple
                    for obj_tuple in obj_tuples
                }
                (
                    uploaded_obj_names,
                    uploaded_obj_upas,
                ) = _query_workspace_with_load_id_mass(
                    ws, workspace_id, load_id, list(name2tuple.keys())
                )

                batch_upas = tuple(uploaded_obj_upas)
                batch_uploaded_tuples = [
                    name2tuple[name] for name in uploaded_obj_names
                ]

            except Exception as e:
                print(
                    f"WARNING: There are inconsistencies between "
                    f"the workspace and the yaml files as the result of {e}\n"
                    f"Run the script again to attempt resolution."
                )

        # post process on sucessful uploads
        for obj_tuple, upa in zip(batch_uploaded_tuples, batch_upas):
            _post_process(
                upload_env_key,
                workspace_id,
                load_id,
                upload_dir,
                output_dir,
                obj_tuple,
                upa,
            )

        uploaded_count += len(batch_uploaded_tuples)
        if uploaded_count % 100 == 0:
            print(
                f"Assemblies uploaded: {uploaded_count}/{assembly_files_len}, "
                f"Percentage: {uploaded_count / assembly_files_len * 100:.2f}%, "
                f"Time: {datetime.now()}"
            )

        if uploaded_fail:
            return uploaded_count

    return uploaded_count


def _dict2tuple_list(objs_dict: dict[str, str]) -> list[_WSObjTuple]:
    ws_object_tuple_list = [
        _WSObjTuple(
            i[0], i[1], os.path.join(_JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER, i[0])
        )
        for i in objs_dict.items()
    ]
    return ws_object_tuple_list


def _gen(
    wait_to_upload_objs: dict[str, str],
    batch_size: int,
) -> Generator[list[_WSObjTuple], None, None]:
    """
    Generator function to yield the object files to upload.
    """
    obj_tuple_list = _dict2tuple_list(wait_to_upload_objs)
    # yield WSObjTuples in batch
    for idx in range(0, len(wait_to_upload_objs), batch_size):
        yield obj_tuple_list[idx: idx + batch_size]


def main():
    parser = _get_parser()
    args = parser.parse_args()

    workspace_id = args.workspace_id
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    source_version = getattr(args, loader_common_names.SOURCE_VER_ARG_NAME)
    root_dir = getattr(args, loader_common_names.ROOT_DIR_ARG_NAME)
    token_filepath = args.token_filepath
    create_assembly_only = args.create_assembly_only
    upload_file_ext = args.upload_file_ext or _get_default_file_ext(create_assembly_only)
    batch_size = args.batch_size
    cbs_max_tasks = args.cbs_max_tasks
    au_service_ver = args.au_service_ver
    keep_job_dir = args.keep_job_dir
    catalog_admin = args.as_catalog_admin
    load_id = args.load_id
    if not load_id:
        print("load_id is not provided. Generating a load_id ...")
        load_id = uuid.uuid4().hex
    print(
        f"load_id is {load_id}.\n"
        f"Please keep using this load version until the load is complete!"
    )

    env = args.env
    kb_base_url = loader_common_names.KB_BASE_URL_MAP[env]

    if workspace_id <= 0:
        parser.error(f"workspace_id needs to be > 0")
    if batch_size <= 0:
        parser.error(f"batch_size needs to be > 0")
    if cbs_max_tasks <= 0:
        parser.error(f"cbs_max_tasks needs to be > 0")

    uid = os.getuid()
    username = os.getlogin()

    job_dir = loader_helper.make_job_dir(root_dir, username)
    collection_source_dir = loader_helper.make_collection_source_dir(
        root_dir, loader_common_names.DEFAULT_ENV, kbase_collection, source_version
    )
    upload_dir = loader_helper.make_collection_source_dir(
        root_dir, env, kbase_collection, source_version
    )
    output_dir = loader_helper.make_sourcedata_ws_dir(root_dir, env, workspace_id)

    proc = None
    conf = None

    try:
        # setup container.conf file for the callback server logs if needed
        if loader_helper.is_config_modification_required():
            if click.confirm(
                f"The config file at {loader_common_names.CONTAINERS_CONF_PATH}\n"
                f"needs to be modified to allow for container logging.\n"
                f"Params 'seccomp_profile' and 'log_driver' will be added/updated under section [containers]. Do so now?\n"
            ):
                loader_helper.setup_callback_server_logs()
            else:
                print("Permission denied and exiting ...")
                return

        # start podman service
        proc = loader_helper.start_podman_service(uid)

        # set up conf for uploader, start callback server, and upload objects to workspace
        conf = Conf(
            job_dir=job_dir,
            kb_base_url=kb_base_url,
            token_filepath=token_filepath,
            max_callback_server_tasks=cbs_max_tasks,
            catalog_admin=catalog_admin,
        )

        count, wait_to_upload_objs = _fetch_objects_to_upload(
            env,
            workspace_id,
            load_id,
            collection_source_dir,
            upload_file_ext,
        )

        # set up workspace client
        ws_url = os.path.join(kb_base_url, "ws")
        ws = Workspace(ws_url, token=conf.token)

        uploaded_obj_names, uploaded_obj_upas = _query_workspace_with_load_id_mass(
            ws, workspace_id, load_id, list(wait_to_upload_objs.keys())
        )
        # fix inconsistencies between the workspace and the local yaml files
        if uploaded_obj_names:
            print("Start failure recovery process ...")
            wait_to_update_objs = {
                obj_name: wait_to_upload_objs[obj_name]
                for obj_name in uploaded_obj_names
            }
            uploaded_tuples = _dict2tuple_list(wait_to_update_objs)
            for obj_tuple, upa in zip(uploaded_tuples, uploaded_obj_upas):
                _post_process(
                    env,
                    workspace_id,
                    load_id,
                    upload_dir,
                    output_dir,
                    obj_tuple,
                    upa,
                )
            # remove objects that are already uploaded
            for obj_name in uploaded_obj_names:
                wait_to_upload_objs.pop(obj_name)
            print("Recovery process completed ...")

        if not wait_to_upload_objs:
            print(
                f"All {count} files already exist in workspace {workspace_id}"
            )
            return

        wtus_len = len(wait_to_upload_objs)
        print(f"Originally planned to upload {count} object files")
        print(f"Detected {count - wtus_len} object files already exist in workspace")

        data_dir = _prepare_skd_job_dir_to_upload(conf, wait_to_upload_objs)
        print(
            f"{wtus_len} objects in {data_dir} are ready to upload to workspace {workspace_id}"
        )

        start = time.time()
        if create_assembly_only:
            uploaded_count = _upload_assembly_files_in_parallel(
                AssemblyUtil(
                    conf.callback_url, service_ver=au_service_ver, token=conf.token
                ),
                ws,
                env,
                workspace_id,
                load_id,
                upload_dir,
                wait_to_upload_objs,
                batch_size,
                output_dir,
            )
        else:
            raise NotImplementedError("Genome object uploader is not implemented yet")

        upload_time = (time.time() - start) / 60
        assy_per_min = uploaded_count / upload_time

        print(
            f"\ntook {upload_time:.2f} minutes to upload {uploaded_count} objects, "
            f"averaging {assy_per_min:.2f} objects per minute"
        )

    finally:
        # stop callback server if it is on
        if conf:
            conf.stop_callback_server()

        # stop podman service if it is on
        if proc:
            proc.terminate()

    if not keep_job_dir:
        shutil.rmtree(job_dir)


if __name__ == "__main__":
    main()
