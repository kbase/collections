"""
usage: workspace_uploader.py [-h] --workspace_id WORKSPACE_ID [--kbase_collection KBASE_COLLECTION] [--source_ver SOURCE_VER]
                             [--root_dir ROOT_DIR] [--token_filepath TOKEN_FILEPATH] [--env {CI,NEXT,APPDEV,PROD}]
                             [--upload_file_ext UPLOAD_FILE_EXT [UPLOAD_FILE_EXT ...]] [--keep_job_dir]

PROTOTYPE - Upload assembly files to the workspace service (WSS).

options:
  -h, --help            show this help message and exit

required named arguments:
  --workspace_id WORKSPACE_ID
                        Workspace addressed by the permanent ID
  --kbase_collection KBASE_COLLECTION
                        Create a collection and link in data to that collection from the overall workspace source data dir
  --source_ver SOURCE_VER
                        Create a source version and link in data to that collection from the overall workspace source data dir

optional arguments:
  --root_dir ROOT_DIR   Root directory. (default: /global/cfs/cdirs/kbase/collections)
  --token_filepath TOKEN_FILEPATH
                        A file path that stores KBase token
  --env {CI,NEXT,APPDEV,PROD}
                        KBase environment, defaulting to PROD (default: PROD)
  --upload_file_ext UPLOAD_FILE_EXT [UPLOAD_FILE_EXT ...]
                        Upload only files that match given extensions (default: ['genomic.fna.gz'])
  --keep_job_dir        Keep SDK job directory after upload task is completed

e.g.
PYTHONPATH=. python src/loaders/workspace_uploader/workspace_uploader.py --workspace_id 69046 --kbase_collection GTDB --source_ver 207 --env CI --keep_job_dir

NOTE:
NERSC file structure for WS:

/global/cfs/cdirs/kbase/collections/sourcedata/ -> WS -> ENV -> workspace ID -> UPA -> .fna.gz file

e.g.
/global/cfs/cdirs/kbase/collections/sourcedata/WS -> CI -> 69046 -> 69046_58_1 -> GCF_000979115.1_gtlEnvA5udCFS_genomic.fna.gz
                                                                    69046_60_1 -> GCF_000970165.1_ASM97016v1_genomic.fna.gz

The data will be linked to the collections source directory:
e.g. /global/cfs/cdirs/kbase/collections/collectionssource/ -> ENV -> kbase_collection -> source_ver -> UPA -> .fna.gz file
"""
import argparse
import docker
import os
import shutil
import time
import uuid
import yaml

from datetime import datetime
from pathlib import Path
from typing import Tuple

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.workspaceClient import Workspace 
from src.loaders.common import loader_common_names, loader_helper
from src.loaders.ncbi_downloader.ncbi_downloader_helper import get_work_dir

# setup KB_AUTH_TOKEN as env or provide a token_filepath in --token_filepath
# export KB_AUTH_TOKEN="your-kb-auth-token"

UPLOAD_FILE_EXT = ["genomic.fna.gz"]  # uplaod only files that match given extensions
JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER = "/kb/module/work/tmp"
DATA_DIR = "DATA_DIR"
UPLOADED_YAML = "uploaded.yaml"


class Conf:
    def __init__(self, job_dir, kb_base_url, token_filepath):
        port = loader_helper.find_free_port()
        token = loader_helper.get_token(token_filepath)
        self.start_callback_server(
            docker.from_env(), uuid.uuid4().hex, job_dir, kb_base_url, token, port
        )
        ws_url = os.path.join(kb_base_url, "ws")
        callback_url = "http://" + loader_helper.get_ip() + ":" + str(port)
        print("callback_url:", callback_url)
        self.ws = Workspace(ws_url, token=token)
        self.asu = AssemblyUtil(callback_url, token=token)

    def setup_callback_server_envs(self, job_dir, kb_base_url, token, port):
        # initiate env and vol
        env = {}
        vol = {}

        # used by the callback server
        env["KB_AUTH_TOKEN"] = token
        env["KB_BASE_URL"] = kb_base_url
        env["JOB_DIR"] = job_dir
        env["CALLBACK_PORT"] = port

        # setup volumes required for docker container
        docker_host = os.environ["DOCKER_HOST"]
        if docker_host.startswith("unix:"):
            docker_host = docker_host[5:]

        vol[job_dir] = {"bind": job_dir, "mode": "rw"}
        vol[docker_host] = {"bind": "/run/docker.sock", "mode": "rw"}

        return env, vol

    def start_callback_server(
        self, client, container_name, job_dir, kb_base_url, token, port
    ):
        env, vol = self.setup_callback_server_envs(job_dir, kb_base_url, token, port)
        self.container = client.containers.run(
            name=container_name,
            image=loader_common_names.CALLBACK_UPLOADER_IMAGE_NAME,
            detach=True,
            network_mode="host",
            environment=env,
            volumes=vol,
        )
        time.sleep(2)

    def stop_callback_server(self):
        self.container.stop()
        self.container.remove()


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
        help="Create a collection and link in data to that collection from the overall workspace source data dir",
    )
    required.add_argument(
        f"--{loader_common_names.SOURCE_VER_ARG_NAME}",
        type=str,
        help="Create a source version and link in data to that collection from the overall workspace source data dir",
    )

    # Optional argument
    optional.add_argument(
        "--root_dir",
        type=str,
        default=loader_common_names.ROOT_DIR,
        help="Root directory.",
    )
    optional.add_argument(
        "--token_filepath",
        type=str,
        help="A file path that stores KBase token",
    )
    optional.add_argument(
        "--env",
        type=str,
        choices=loader_common_names.KB_ENV,
        default="PROD",
        help="KBase environment, defaulting to PROD",
    )
    optional.add_argument(
        "--upload_file_ext",
        type=str,
        nargs="+",
        default=UPLOAD_FILE_EXT,
        help="Upload only files that match given extensions",
    )
    optional.add_argument(
        "--keep_job_dir",
        action="store_true",
        help="Keep SDK job directory after upload task is completed",
    )
    return parser


def _get_yaml_file_path(root_dir: str, assembly_dir_name: str) -> str:
    """
    Get the uploaded.yaml file path from NCBI/sourcedata directory.
    """
    file_path = os.path.join(get_work_dir(root_dir), assembly_dir_name, UPLOADED_YAML)
    Path(file_path).touch(exist_ok=True)
    return file_path


def _sanitize_data_dir(job_dir: str) -> str:
    """
    Create a temporary directory for storing uploaded files.
    """
    data_dir = os.path.join(job_dir, "workdir/tmp", DATA_DIR)
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir)
    return data_dir


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
    workspace_name: str,
    file_path: str,
    assembly_name: str,
) -> None:
    """Upload an assembly file to workspace."""
    success, attempts, max_attempts = False, 0, 3
    while attempts < max_attempts and not success:
        try:
            time.sleep(attempts)
            conf.asu.save_assembly_from_fasta(
                {
                    "file": {"path": file_path},
                    "workspace_name": workspace_name,
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


def _read_yaml_file(
        root_dir: str,
        env: str,
        workspace_id: int,
        assembly_dir_name: str,
        assembly_name: str, 
) -> Tuple[dict[str, dict[int, list[str]]], bool]:
    """
    Get metadata and upload status of an assembly from the uploaded.yaml file.
    """

    uploaded = False
    if env not in loader_common_names.KB_ENV:
        raise ValueError(f"Currently only support these {loader_common_names.KB_ENV} envs for upload")

    file_path = _get_yaml_file_path(root_dir, assembly_dir_name)

    with open(file_path, "r") as file:
        data = yaml.safe_load(file)

    if not data:
        data = {env: dict()}

    if workspace_id not in data[env]:
        data[env][workspace_id] = list()

    assembly_dicts = data[env][workspace_id]
    if assembly_name in [assembly_dict["file_name"] for assembly_dict in assembly_dicts]:
        uploaded = True
    return data, uploaded


def _update_yaml_file(
        root_dir: str,
        env: str,
        workspace_id: int,
        upa: str,
        assembly_dir_name: str,
        assembly_name: str,
) -> None:
    """
    Update the uploaded.yaml file in target genome_dir with newly uploaded assembly names and upa info.
    """
    data, uploaded = _read_yaml_file(root_dir, env, workspace_id, assembly_dir_name, assembly_name)

    if uploaded:
        raise ValueError(f"Assembly {assembly_name} already exists in workspace {workspace_id}")

    data_dict = {"file_name": assembly_name, "upa": upa}
    data[env][workspace_id].append(data_dict)

    file_path = _get_yaml_file_path(root_dir, assembly_dir_name)
    with open(file_path, "w") as file:
        yaml.dump(data, file)


def _fetch_assemblies_to_upload(
        root_dir: str,
        env: str,
        workspace_id: int,
        collection_source_dir: str,
        upload_file_ext: list[str],
) -> Tuple[int, dict[str, str]]:
    """
    Help fetch assemblies to upload.
    """
    count = 0
    wait_to_upload_assemblies = dict()

    assembly_dirs = [
        os.path.join(collection_source_dir, d)
        for d in os.listdir(collection_source_dir)
        if os.path.isdir(os.path.join(collection_source_dir, d))
    ]

    for assembly_dir in assembly_dirs:
        assembly_files = [
            f
            for f in os.listdir(assembly_dir)
            if os.path.isfile(os.path.join(assembly_dir, f))
        ]

        assembly_dir_name = os.path.basename(assembly_dir)

        for assembly_file in assembly_files:

            if assembly_file.endswith(tuple(upload_file_ext)):

                count += 1
                _, uploaded = _read_yaml_file(root_dir, env, workspace_id, assembly_dir_name, assembly_file)

                if uploaded:
                    print(
                        f"Assembly {assembly_file} already exists in workspace {workspace_id}. Skipping."
                    )
                    continue

                wait_to_upload_assemblies[assembly_file] = assembly_dir

    return count, wait_to_upload_assemblies


def _prepare_skd_job_dir_to_upload(job_dir: str, wait_to_upload_assemblies: dict[str, str]) -> str:
    """
    Prepare SDK job directory to upload.
    """
    data_dir = _sanitize_data_dir(job_dir)
    for assembly_file, assembly_dir in wait_to_upload_assemblies.items():
        src_file = _get_source_file(assembly_dir, assembly_file)
        dest_file = os.path.join(data_dir, assembly_file)
        loader_helper.create_hardlink_between_files(dest_file, src_file)

    return data_dir


def _get_assembly_name_upa_mapping(conf: Conf, workspace_id: int) -> dict[str, str]:
    """
    Get a mapping of UPA to assembly name from target workspace.
    """
    assembly_objs = loader_helper.list_objects(
        workspace_id, conf, loader_common_names.OBJECTS_NAME_ASSEMBLY
    )
    hashmap = {obj_info[1]: "{6}_{0}_{4}".format(*obj_info) for obj_info in assembly_objs}
    return hashmap
 

def create_entries_in_sourcedata_workspace(
        root_dir: str,
        env: str,
        workspace_id: int,
        assembly_names: list[str],
        assembly_name_to_dir: dict[str, str],
        assembly_name_to_upa: dict[str, str],
        output_dir: str,
) -> list[str]:
    """
    Create a standard entry in sourcedata/workspace for each assembly.
    Hardlink to the original assembly file in sourcedata to avoid duplicating the file.
    Update the uploaded.yaml file in the genome directory with assembly names and upa info.

    root_dir: root directory
    env: KBase environment
    workspace_id: target workspace addressed by the permanent ID
    assembly_names: a list of assembly names newly uploaded to the target workspace
    assembly_name_to_dir: a dictionary of assembly name to its directory path
    assembly_name_to_upa: a dictionary of assembly name to its UPA
    output_dir: output directory to create entries in workspace
    """
    upas = list()
    for assembly_name in assembly_names:
        try:
            assembly_dir = assembly_name_to_dir[assembly_name]
        except KeyError as e:
            raise ValueError(f"Unable to find assembly {assembly_name}") from e

        src_file = _get_source_file(assembly_dir, assembly_name)
        assembly_dir_name = os.path.basename(assembly_dir)

        try:
            upa = assembly_name_to_upa[assembly_name]
        except KeyError as e:
            raise ValueError(f"Unable to find assembly {assembly_name} from target workspace") from e
        
        upa_dir = os.path.join(output_dir, upa)
        os.makedirs(upa_dir, exist_ok=True)
        upas.append(upa)

        dest_file = os.path.join(upa_dir, assembly_name)
        loader_helper.create_hardlink_between_files(dest_file, src_file)
        _update_yaml_file(root_dir, env, workspace_id, upa, assembly_dir_name, assembly_name)

    return upas


def upload_assemblies_to_workspace(
        conf: Conf,
        workspace_name: str,
        data_dir: str,
) -> list[str]:
    """
    Upload assemblies to workspace and record failed assembly names.

    conf: Conf object
    workspace_name: a string used as a name for a workspace
    data_dir: directory of assemblies to upload
    """

    assembly_files = os.listdir(data_dir)
    print(f'start uploading {len(assembly_files)} assembly files')

    failed_names = list()

    counter = 1
    for assembly_name in assembly_files:

        if counter % 5000 == 0:
            print(f"{round(counter / len(assembly_files), 4) * 100}% finished at {datetime.now()}")

        assembly_path = os.path.join(JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER, DATA_DIR, assembly_name)
        try:
            _upload_assembly_to_workspace(conf, workspace_name, assembly_path, assembly_name)
        except Exception as e:
            print(e)
            failed_names.append(assembly_name)

        counter += 1

    if failed_names:
        print(f'Failed to upload {failed_names}')

    return failed_names


def main():
    parser = _get_parser()
    args = parser.parse_args()

    workspace_id = args.workspace_id
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    source_version = getattr(args, loader_common_names.SOURCE_VER_ARG_NAME)
    root_dir = args.root_dir
    token_filepath = args.token_filepath
    upload_file_ext = args.upload_file_ext
    keep_job_dir = args.keep_job_dir

    env = args.env
    kb_base_url = loader_common_names.KB_BASE_URL_MAP[env]

    if workspace_id <= 0:
        parser.error(f"workspace_id needs to be > 0")

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
    except Exception as e:
        print(e)
    else:
        # set up conf, start callback server, and upload assemblies to workspace
        conf = Conf(job_dir, kb_base_url, token_filepath)
        workspace_name = conf.ws.get_workspace_info({"id": workspace_id})[1]

        count, wait_to_upload_assemblies = _fetch_assemblies_to_upload(
            root_dir,
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

        start = time.time()
        data_dir = _prepare_skd_job_dir_to_upload(job_dir, wait_to_upload_assemblies)
        failed_names = upload_assemblies_to_workspace(conf, workspace_name, data_dir)

        if failed_names:
            print(f"\nFailed to upload {failed_names}")
        
        assembly_count = wtus_len - len(failed_names)
        upload_speed = (time.time() - start) / assembly_count
        print(f"\nSuccessfully upload {assembly_count} assemblies, average {upload_speed:.2f}s/assembly.")

        new_assembly_names = [name for name in wait_to_upload_assemblies if name not in failed_names]
        assembly_name_to_upa = _get_assembly_name_upa_mapping(conf, workspace_id)
        upas = create_entries_in_sourcedata_workspace(
            root_dir,
            env,
            workspace_id,
            new_assembly_names,
            wait_to_upload_assemblies,
            assembly_name_to_upa,
            output_dir,
        )
        loader_helper.create_softlinks_in_collection_source_dir(upload_dir, output_dir, upas)

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
