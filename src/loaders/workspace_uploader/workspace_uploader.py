"""
usage: workspace_uploader.py [-h] --workspace_id WORKSPACE_ID [--kbase_collection KBASE_COLLECTION] [--source_ver SOURCE_VER]
                             [--root_dir ROOT_DIR] [--token_filepath TOKEN_FILEPATH] [--env {CI,NEXT,APPDEV,PROD}]
                             [--upload_file_ext UPLOAD_FILE_EXT [UPLOAD_FILE_EXT ...]] [--workers WORKERS] [--keep_job_dir]

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
  --workers WORKERS     Number of workers for multiprocessing (default: 5)
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
import os
import shutil
import time
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path
from typing import Tuple

import yaml

from src.loaders.common import loader_common_names, loader_helper
from src.loaders.common.callback_server_wrapper import Conf

# setup KB_AUTH_TOKEN as env or provide a token_filepath in --token_filepath
# export KB_AUTH_TOKEN="your-kb-auth-token"

UPLOAD_FILE_EXT = ["genomic.fna.gz"]  # uplaod only files that match given extensions
JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER = "/kb/module/work/tmp"
UPLOADED_YAML = "uploaded.yaml"


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
        "--workers",
        type=int,
        default=5,
        help="Number of workers for multiprocessing",
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


def _upload_assembly_to_workspace(
    conf: Conf,
    workspace_id: int,
    file_path: str,
    assembly_name: str,
) -> str:
    """Upload an assembly file to workspace."""
    success, attempts, max_attempts = False, 0, 3
    while attempts < max_attempts and not success:
        try:
            time.sleep(attempts)
            assembly_ref = conf.asu.save_assembly_from_fasta2(
                {
                    "file": {"path": file_path},
                    "workspace_id": workspace_id,
                    "assembly_name": assembly_name,
                }
            )
            success = True
        except Exception as e:
            print(f"Error:\n{e}\nfrom attempt {attempts + 1}.\nTrying to rerun.")
            attempts += 1

    if not success:
        raise ValueError(
            f"Upload Failed for {file_path} after {max_attempts} attempts!"
        )

    upa = assembly_ref["upa"].replace("/", "_")
    return upa


def _read_upload_status_yaml_file(
        upload_env_key: str,
        workspace_id: int,
        assembly_dir: str,
        assembly_name: str, 
) -> Tuple[dict[str, dict[int, list[str]]], bool]:
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
) -> Tuple[int, dict[str, str]]:
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


def _process_input(conf: Conf) -> None:
    """
    Process input from input_queue and put the result in output_queue.
    """
    while True:
        task = conf.input_queue.get(block=True)
        if not task:
            print("Stopping")
            break

        upa = None
        (
            upload_env_key,
            workspace_id,
            container_internal_assembly_path,
            host_assembly_dir,
            assembly_name,
            upload_dir,
            counter,
            assembly_files_len,
        ) = task

        try:
            upa = _upload_assembly_to_workspace(
                conf, workspace_id, container_internal_assembly_path, assembly_name
            )
            _post_process(
                upload_env_key,
                workspace_id,
                host_assembly_dir,
                assembly_name,
                upload_dir,
                conf.output_dir,
                upa
            )
        except Exception as e:
            print(f"Failed assembly name: {assembly_name}. Exception:")
            print(e)

        conf.output_queue.put((assembly_name, upa))

        if counter % 3000 == 0:
            print(f"Assemblies processed: {counter}/{assembly_files_len}, "
                  f"Percentage: {counter / assembly_files_len * 100:.2f}%, "
                  f"Time: {datetime.now()}")


def _upload_assembly_files_in_parallel(
        conf: Conf,
        upload_env_key: str,
        workspace_id: int,
        upload_dir: str,
        wait_to_upload_assemblies: dict[str, str],
) -> list[str]:
    """
    Upload assembly files to the target workspace in parallel using multiprocessing.

    Parameters:
        conf: Conf object
        upload_env_key: environment variable key in uploaded.yaml file
        workspace_id: target workspace id
        upload_dir: a directory in collectionssource that creates new directories linking to sourcedata
        wait_to_upload_assemblies: a dictionary that maps assembly file name to assembly directory

    Returns:
        a list of assembly names that failed to upload
    """
    assembly_files_len = len(wait_to_upload_assemblies)
    print(f"Start uploading {assembly_files_len} assembly files with {conf.workers} workers\n")

    # Put the assembly files in the input_queue
    counter = 1
    for assembly_name, host_assembly_dir in wait_to_upload_assemblies.items():

        container_internal_assembly_path = os.path.join(
            JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER, assembly_name
        )
        conf.input_queue.put(
            (
                upload_env_key,
                workspace_id,
                container_internal_assembly_path,
                host_assembly_dir,
                assembly_name,
                upload_dir,
                counter,
                assembly_files_len,
            )
        )

        if counter % 5000 == 0:
            print(f"Jobs added to the queue: {counter}/{assembly_files_len}, "
                  f"Percentage: {counter / assembly_files_len * 100:.2f}%, "
                  f"Time: {datetime.now()}")

        counter += 1

    # Signal the workers to terminate when they finish uploading assembly files
    for _ in range(conf.workers):
        conf.input_queue.put(None)

    results = [conf.output_queue.get() for _ in range(assembly_files_len)]
    failed_names = [assembly_name for assembly_name, upa in results if upa is None]

    # Close and join the processes
    conf.pools.close()
    conf.pools.join()

    return failed_names


def main():
    parser = _get_parser()
    args = parser.parse_args()

    workspace_id = args.workspace_id
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    source_version = getattr(args, loader_common_names.SOURCE_VER_ARG_NAME)
    root_dir = getattr(args, loader_common_names.ROOT_DIR_ARG_NAME)
    token_filepath = args.token_filepath
    upload_file_ext = args.upload_file_ext
    workers = args.workers
    keep_job_dir = args.keep_job_dir

    env = args.env
    kb_base_url = loader_common_names.KB_BASE_URL_MAP[env]

    if workspace_id <= 0:
        parser.error(f"workspace_id needs to be > 0")
    if workers < 1 or workers > cpu_count():
        parser.error(f"minimum worker is 1 and maximum worker is {cpu_count()}")

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
        # start podman service
        proc = loader_helper.start_podman_service(uid)

        # set up conf, start callback server, and upload assemblies to workspace
        conf = Conf(
            job_dir,
            output_dir,
            _process_input,
            kb_base_url,
            token_filepath,
            workers,
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

        start = time.time()
        failed_names = _upload_assembly_files_in_parallel(
            conf,
            env,
            workspace_id,
            upload_dir,
            wait_to_upload_assemblies,
        )
        
        assembly_count = wtus_len - len(failed_names)
        upload_time = (time.time() - start) / 60
        assy_per_min = assembly_count / upload_time

        print(f"\n{workers} workers took {upload_time:.2f} minutes to upload {assembly_count} assemblies, "
              f"averaging {assy_per_min:.2f} assemblies per minute")

        if failed_names:
            raise ValueError(f"\nFailed to upload {failed_names}")

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
