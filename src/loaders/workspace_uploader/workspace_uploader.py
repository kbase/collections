"""
usage: workspace_uploader.py [-h] --workspace_id WORKSPACE_ID [--kbase_collection KBASE_COLLECTION] [--source_ver SOURCE_VER]
                             [--root_dir ROOT_DIR] [--token_filepath TOKEN_FILEPATH] [--env {CI,NEXT,APPDEV,PROD}]
                             [--upload_file_ext UPLOAD_FILE_EXT [UPLOAD_FILE_EXT ...]] [--batch_size BATCH_SIZE]
                             [--jr_max_tasks JR_MAX_TASKS] [--au_service_ver AU_SERVICE_VER] [--keep_job_dir]

PROTOTYPE - Upload assembly files to the workspace service (WSS).

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
  --token_filepath TOKEN_FILEPATH
                        A file path that stores a KBase token appropriate for the KBase environment
                        If not provided, the token must be provided in the `KB_AUTH_TOKEN` environment variable
  --env {CI,NEXT,APPDEV,PROD}
                        KBase environment (default: PROD)
  --upload_file_ext UPLOAD_FILE_EXT [UPLOAD_FILE_EXT ...]
                        Upload only files that match given extensions (default: ['genomic.fna.gz'])
  --batch_size BATCH_SIZE
                        Number of files to upload per batch (default: 1000)
  --jr_max_tasks JR_MAX_TASKS
                        The maxmium subtasks for the callback server (default: 20)
  --au_service_ver AU_SERVICE_VER
                        The service version of AssemblyUtil client('dev', 'beta', 'release', or a git commit) (default: release)
  --keep_job_dir        Keep SDK job directory after upload task is completed

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
from collections import namedtuple
from datetime import datetime
from pathlib import Path
from typing import Generator

import yaml

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_common_names, loader_helper
from src.loaders.common.callback_server_wrapper import Conf

# setup KB_AUTH_TOKEN as env or provide a token_filepath in --token_filepath
# export KB_AUTH_TOKEN="your-kb-auth-token"

UPLOAD_FILE_EXT = ["genomic.fna.gz"]  # uplaod only files that match given extensions
JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER = "/kb/module/work/tmp"
UPLOADED_YAML = "uploaded.yaml"

AssemblyTuple = namedtuple("AssemblyTuple", ["assembly_name", "host_assembly_dir", "container_internal_assembly_path"])


def _get_parser():
    parser = argparse.ArgumentParser(
        description="PROTOTYPE - Upload assembly files to the workspace service (WSS).",
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
        help=loader_common_names.ROOT_DIR_DESCR
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
        "--upload_file_ext",
        type=str,
        nargs="+",
        default=UPLOAD_FILE_EXT,
        help="Upload only files that match given extensions",
    )
    optional.add_argument(
        "--batch_size",
        type=int,
        default=2500,
        help="Number of files to upload per batch",
    )
    optional.add_argument(
        "--jr_max_tasks",
        type=int,
        default=20,
        help="The maxmium subtasks for the callback server",
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
    return parser


def _get_yaml_file_path(assembly_dir: str) -> str:
    """
    Get the uploaded.yaml file path from collections source directory.
    """
    file_path = os.path.join(assembly_dir, UPLOADED_YAML)
    Path(file_path).touch(exist_ok=True)
    return file_path


def _get_source_file(assembly_dir: str, assembly_file: str) -> str:
    """
    Get the sourcedata file path from the assembly directory.
    """
    if not os.path.islink(assembly_dir):
        raise ValueError(f"{assembly_dir} is not a symlink")
    src_file = os.path.join(os.readlink(assembly_dir), assembly_file)
    return src_file


def _upload_assemblies_to_workspace(
        asu: AssemblyUtil,
        workspace_id: int,
        assembly_tuples: list[AssemblyTuple],
) -> tuple[str, ...]:
    """
    Upload assembly files to the target workspace in batch. The bulk method fails
    and an error will be thrown if any of the assembly files in batch fails to upload.
    """
    inputs = [
        {
            "file": assembly_tuple.container_internal_assembly_path,
            "assembly_name": assembly_tuple.assembly_name,
        }
        for assembly_tuple in assembly_tuples
    ]

    try:
        assembly_ref = asu.save_assemblies_from_fastas(
            {
                "workspace_id": workspace_id,
                "inputs": inputs
            }
        )
    except Exception as e:
        print(e)
        raise e

    upas = tuple([result_dict["upa"].replace("/", "_")
                  for result_dict in assembly_ref["results"]])
    return upas


def _read_upload_status_yaml_file(
        upload_env_key: str,
        workspace_id: int,
        assembly_dir: str,
        assembly_name: str, 
) -> tuple[dict[str, dict[int, list[str]]], bool]:
    """
    Get metadata and upload status of an assembly from the uploaded.yaml file.
    """

    uploaded = False
    if upload_env_key not in loader_common_names.KB_ENV:
        raise ValueError(f"Currently only support these {loader_common_names.KB_ENV} envs for upload")

    file_path = _get_yaml_file_path(assembly_dir)

    with open(file_path, "r") as file:
        data = yaml.safe_load(file)

    if not data:
        data = {upload_env_key: dict()}

    if workspace_id not in data[upload_env_key]:
        data[upload_env_key][workspace_id] = dict()

    assembly_dict = data[upload_env_key][workspace_id]
    if assembly_dict and assembly_dict["file_name"] == assembly_name:
        uploaded = True
    return data, uploaded


def _update_upload_status_yaml_file(
        upload_env_key: str,
        workspace_id: int,
        upa: str,
        assembly_dir: str,
        assembly_name: str,
) -> None:
    """
    Update the uploaded.yaml file in target genome_dir with newly uploaded assembly names and upa info.
    """
    data, uploaded = _read_upload_status_yaml_file(upload_env_key, workspace_id, assembly_dir, assembly_name)

    if uploaded:
        raise ValueError(f"Assembly {assembly_name} already exists in workspace {workspace_id}")

    data[upload_env_key][workspace_id] = {"file_name": assembly_name, "upa": upa}

    file_path = _get_yaml_file_path(assembly_dir)
    with open(file_path, "w") as file:
        yaml.dump(data, file)


def _fetch_assemblies_to_upload(
        upload_env_key: str,
        workspace_id: int,
        collection_source_dir: str,
        upload_file_ext: list[str],
) -> tuple[int, dict[str, str]]:
    count = 0
    wait_to_upload_assemblies = dict()

    assembly_dirs = [
        os.path.join(collection_source_dir, d)
        for d in os.listdir(collection_source_dir)
        if os.path.isdir(os.path.join(collection_source_dir, d))
    ]

    for assembly_dir in assembly_dirs:

        assembly_file_list = [
            f
            for f in os.listdir(assembly_dir)
            if os.path.isfile(os.path.join(assembly_dir, f))
            and f.endswith(tuple(upload_file_ext))
        ]

        if len(assembly_file_list) != 1:
            raise ValueError(f"One and only one assembly file that ends with {upload_file_ext} "
                    f"must be present in {assembly_dir} directory")

        count += 1
        assembly_name = assembly_file_list[0]

        _, uploaded = _read_upload_status_yaml_file(upload_env_key, workspace_id, assembly_dir, assembly_name)

        if uploaded:
            print(
                f"Assembly {assembly_name} already exists in workspace {workspace_id}. Skipping."
            )
            continue

        wait_to_upload_assemblies[assembly_name] = assembly_dir

    return count, wait_to_upload_assemblies


def _prepare_skd_job_dir_to_upload(conf: Conf, wait_to_upload_assemblies: dict[str, str]) -> str:
    """
    Prepare SDK job directory to upload.
    """
    for assembly_file, assembly_dir in wait_to_upload_assemblies.items():
        src_file = _get_source_file(assembly_dir, assembly_file)
        dest_file = os.path.join(conf.job_data_dir, assembly_file)
        loader_helper.create_hardlink_between_files(dest_file, src_file)

    return conf.job_data_dir


def _post_process(
        upload_env_key: str,
        workspace_id: int,
        host_assembly_dir: str,
        assembly_name: str,
        upload_dir: str,
        output_dir: str,
        upa: str,
) -> None:
    """
    Create a standard entry in sourcedata/workspace for each assembly.
    Hardlink to the original assembly file in sourcedata to avoid duplicating the file.
    Update the uploaded.yaml file in the genome directory with the assembly name and upa info.
    Creates a softlink from new_dir in collectionssource to the contents of target_dir in sourcedata.
    """
    # Create a standard entry in sourcedata/workspace
    # hardlink to the original assembly file in sourcedata
    src_file = _get_source_file(host_assembly_dir, assembly_name)
    target_dir = os.path.join(output_dir, upa)
    os.makedirs(target_dir, exist_ok=True)
    dest_file = os.path.join(target_dir, f"{upa}.fna.gz")
    loader_helper.create_hardlink_between_files(dest_file, src_file)

    # Update the uploaded.yaml file
    _update_upload_status_yaml_file(upload_env_key, workspace_id, upa, host_assembly_dir, assembly_name)

    # Creates a softlink from new_dir to the contents of upa_dir.
    new_dir = os.path.join(upload_dir, upa)
    loader_helper.create_softlink_between_dirs(new_dir, target_dir)


def _upload_assembly_files_in_parallel(
        asu: AssemblyUtil,
        ws: Workspace,
        upload_env_key: str,
        workspace_id: int,
        upload_dir: str,
        wait_to_upload_assemblies: dict[str, str],
        batch_size: int,
        output_dir: str,
) -> list[str]:
    """
    Upload assembly files to the target workspace in parallel using multiprocessing.

    Parameters:
        asu: AssemblyUtil client
        ws: Workspace client
        upload_env_key: environment variable key in uploaded.yaml file
        workspace_id: target workspace id
        upload_dir: a directory in collectionssource that creates new directories linking to sourcedata
        wait_to_upload_assemblies: a dictionary that maps assembly file name to assembly directory
        batch_size: a number of files to upload per batch
        output_dir: a directory in sourcedata/workspace to store new assembly entries

    Returns:
        number of assembly files have been sucessfully uploaded from wait_to_upload_assemblies
    """
    assembly_files_len = len(wait_to_upload_assemblies)
    print(f"Start uploading {assembly_files_len} assembly files\n")

    uploaded_count = 0
    uploaded_fail = False
    for assembly_tuples in _gen(wait_to_upload_assemblies, batch_size):

        try:
            batch_upas = _upload_assemblies_to_workspace(asu, workspace_id, assembly_tuples)
            batch_uploaded_tuples = assembly_tuples
        except Exception as e:
            name_assemblyTuple_map = {
                assembly_tuple.assembly_name: assembly_tuple for assembly_tuple in assembly_tuples
            }
            assembly_objects_in_ws = loader_helper.list_objects(
                workspace_id,
                ws,
                loader_common_names.OBJECTS_NAME_ASSEMBLY,
            )
            print("assembly_objects_in_ws: ", assembly_objects_in_ws)
            name_upa_map = {
                object_info[1]: "{6}_{0}_{4}".format(*object_info) for object_info in assembly_objects_in_ws
            }
            batch_uploaded_objects_names = set(name_upa_map.keys()) & set(name_assemblyTuple_map.keys())

            # update batch_uploaded_tuples and batch_upas
            batch_upas = tuple([name_upa_map[name] for name in batch_uploaded_objects_names])
            batch_uploaded_tuples = [name_assemblyTuple_map[name] for name in batch_uploaded_objects_names]
            uploaded_fail = True

        for assembly_tuple, upa in zip(batch_uploaded_tuples, batch_upas):
            _post_process(
                upload_env_key,
                workspace_id,
                assembly_tuple.host_assembly_dir,
                assembly_tuple.assembly_name,
                upload_dir,
                output_dir,
                upa
            )

        uploaded_count += len(batch_uploaded_tuples)
        if uploaded_count % 100 == 0:
            print(f"Assemblies uploaded: {uploaded_count}/{assembly_files_len}, "
                  f"Percentage: {uploaded_count / assembly_files_len * 100:.2f}%, "
                  f"Time: {datetime.now()}")

        if uploaded_fail:
            return uploaded_count

    return uploaded_count


def _gen(
        wait_to_upload_assemblies: dict[str, str],
        batch_size: int,
) -> Generator[list[AssemblyTuple], None, None]:
    """
    Generator function to yield the assembly files to upload.
    """
    assembly_files_len = len(wait_to_upload_assemblies)
    assembly_names = tuple(wait_to_upload_assemblies.keys())
    host_assembly_dirs = tuple(wait_to_upload_assemblies.values())
    container_internal_assembly_paths = tuple(
        [
            os.path.join(JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER, assembly_name)
            for assembly_name in assembly_names
        ]
    )

    # construct a list of nametuples to ensure consistency
    assemblyTuple_list = [
        AssemblyTuple(
            assembly_names[idx],
            host_assembly_dirs[idx],
            container_internal_assembly_paths[idx],
        )
        for idx in range(assembly_files_len)
    ]

    # yield AssemblyTuples in batch
    for idx in range(0, assembly_files_len, batch_size):
        yield assemblyTuple_list[idx: idx + batch_size]


def main():
    parser = _get_parser()
    args = parser.parse_args()

    workspace_id = args.workspace_id
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    source_version = getattr(args, loader_common_names.SOURCE_VER_ARG_NAME)
    root_dir = getattr(args, loader_common_names.ROOT_DIR_ARG_NAME)
    token_filepath = args.token_filepath
    upload_file_ext = args.upload_file_ext
    batch_size = args.batch_size
    max_task = args.jr_max_tasks
    au_service_ver = args.au_service_ver
    keep_job_dir = args.keep_job_dir

    env = args.env
    kb_base_url = loader_common_names.KB_BASE_URL_MAP[env]

    if workspace_id <= 0:
        parser.error(f"workspace_id needs to be > 0")
    if batch_size <= 0:
        parser.error(f"batch_size needs to be > 0")
    if max_task <= 0:
        parser.error(f"jr_max_tasks needs to be > 0")

    uid = os.getuid()
    username = os.getlogin()

    job_dir = loader_helper.make_job_dir(root_dir, username)
    collection_source_dir = loader_helper.make_collection_source_dir(
        root_dir, loader_common_names.DEFAULT_ENV, kbase_collection, source_version
    )
    upload_dir = loader_helper.make_collection_source_dir(root_dir, env, kbase_collection, source_version)
    output_dir = loader_helper.make_sourcedata_ws_dir(root_dir, env, workspace_id)

    proc = None
    conf = None

    try:
        # setup container.conf file for the callback server logs if needed
        if loader_helper.is_config_modification_required():
            if click.confirm(f"Set up a containers.conf file at {loader_common_names.CONTAINERS_CONF_PATH} if does not exist already?\n"
                             f"Params 'seccomp_profile' and 'log_driver' will be added/updated under section [containers]"):
                loader_helper.setup_callback_server_logs()
            else:
                print("Permission denied and exiting ...")
                return

        # start podman service
        proc = loader_helper.start_podman_service(uid)

        # set up conf for uploader, start callback server, and upload assemblies to workspace
        conf = Conf(
            job_dir=job_dir,
            output_dir=output_dir,
            kb_base_url=kb_base_url,
            token_filepath=token_filepath,
            au_service_ver=au_service_ver,
            max_task=max_task,
            workspace_downloader=False,
        )

        count, wait_to_upload_assemblies = _fetch_assemblies_to_upload(
            env, 
            workspace_id, 
            collection_source_dir,
            upload_file_ext,
        )

        if not wait_to_upload_assemblies:
            print(f"All {count} assembly files already exist in workspace {workspace_id}")
            return

        wtus_len = len(wait_to_upload_assemblies)
        print(f"Originally planned to upload {count} assembly files")
        print(f"Detected {count - wtus_len} assembly files already exist in workspace")

        data_dir = _prepare_skd_job_dir_to_upload(conf, wait_to_upload_assemblies)
        print(f"{wtus_len} assemblies in {data_dir} are ready to upload to workspace {workspace_id}")

        ws = Workspace(conf.ws_url, token=conf.token)
        asu = AssemblyUtil(
            conf.callback_url, service_ver=au_service_ver, token=conf.token
        )
        start = time.time()
        uploaded_count = _upload_assembly_files_in_parallel(
            asu,
            ws,
            env,
            workspace_id,
            upload_dir,
            wait_to_upload_assemblies,
            batch_size,
            output_dir,
        )

        upload_time = (time.time() - start) / 60
        assy_per_min = uploaded_count / upload_time

        print(f"\ntook {upload_time:.2f} minutes to upload {uploaded_count} assemblies, "
              f"averaging {assy_per_min:.2f} assemblies per minute")

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
