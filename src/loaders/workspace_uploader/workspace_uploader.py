import argparse
import docker
import os
import shutil
import time
import uuid

from datetime import datetime
from typing import Tuple

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_common_names, loader_helper

# setup KB_AUTH_TOKEN as env or provide a token_filepath in --token_filepath
# export KB_AUTH_TOKEN="your-kb-auth-token"

UPLOAD_FILE_EXT = ["genomic.fna.gz"]  # uplaod only files that match given extensions


class Conf:
    def __init__(self, job_dir, kb_base_url, csd, token_filepath):
        port = loader_helper.find_free_port()
        token = loader_helper.get_token(token_filepath)
        self.start_callback_server(
            docker.from_env(), uuid.uuid4().hex, job_dir, kb_base_url, csd, token, port
        )
        ws_url = os.path.join(kb_base_url, "ws")
        callback_url = "http://" + loader_helper.get_ip() + ":" + str(port)
        print("callback_url:", callback_url)
        self.ws = Workspace(ws_url, token=token)
        self.asu = AssemblyUtil(callback_url, token=token)

    def setup_callback_server_envs(self, job_dir, kb_base_url, csd, token, port):
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
        vol[csd] = {"bind": csd, "mode": "rw"}

        return env, vol

    def start_callback_server(
        self, client, container_name, job_dir, kb_base_url, csd, token, port
    ):
        env, vol = self.setup_callback_server_envs(job_dir, kb_base_url, csd, token, port)
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
        description="PROTOTYPE - Download genome files from the workspace service (WSS).",
        formatter_class=loader_helper.ExplicitDefaultsHelpFormatter,
    )

    required = parser.add_argument_group("required named arguments")
    optional = parser.add_argument_group("optional arguments")

    # Required flag argument
    required.add_argument(
        "--workspace_id",
        required=True,
        type=int,
        help="Workspace addressed by the permanent ID",
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
        "--overwrite", action="store_true", help="Overwrite existing files in workspace"
    )
    optional.add_argument(
        "--keep_job_dir",
        action="store_true",
        help="Keep SDK job directory after download task is completed",
    )
    return parser


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


def _get_upa_assembly_name_mapping(conf: Conf, workspace_id: int) -> dict[str, str]:
    """
    Helper function to get a mapping of UPA to assembly name.
    """
    assembly_objs = loader_helper.list_objects(
        workspace_id, conf, loader_common_names.OBJECTS_NAME_ASSEMBLY
    )
    hashmap = {
        "{6}_{0}_{4}".format(*obj_info): obj_info[1] for obj_info in assembly_objs
    }
    return hashmap


def _fetch_assemblies_to_upload(
    workspace_name: str,
    csd: str,
    uploaded_assembly_names: list[str],
    upload_file_ext: list[str],
    overwrite: bool = False,
) -> Tuple(dict[str, str], dict[str, str]):
    """
    Helper function to help fetch assemblies to upload.
    """
    all_assemblies = dict()
    wait_to_upload_assemblies = dict()

    assembly_dirs = [
        os.path.join(csd, d)
        for d in os.listdir(csd)
        if os.path.isdir(os.path.join(csd, d))
    ]

    for assembly_dir in assembly_dirs:
        assembly_files = [
            f
            for f in os.listdir(assembly_dir)
            if os.path.isfile(os.path.join(assembly_dir, f))
        ]

        for assembly_file in assembly_files:

            assembly_file_path = os.path.join(assembly_dir, assembly_file)
            all_assemblies[assembly_file] = assembly_file_path

            if assembly_file in uploaded_assembly_names and not overwrite:
                print(
                    f"Assembly {assembly_file} already exists in workspace {workspace_name}. Skipping."
                )
                continue

            if assembly_file.endswith(tuple(upload_file_ext)):
                wait_to_upload_assemblies[assembly_file] = assembly_file_path


    return all_assemblies, wait_to_upload_assemblies


def create_entries_in_sd_workspace(
    conf: Conf,
    workspace_id: int,
    success_dict: dict[str, str],
    output_dir: str,
) -> None:
    """
    Create a standard entry in sourcedata/workspace for each assembly.
    Hardlink to the original assembly file in sourcedata to avoid duplicating the file.

    conf: Conf object
    workspace_id: Workspace addressed by the permanent ID
    success_dict: a dictionary of assembly name maps to file path
    output_dir: output directory
    """
    upa_assembly_mapping = _get_upa_assembly_name_mapping(conf, workspace_id)
    for upa, assembly_name in upa_assembly_mapping.items():
        try:
            src_file = success_dict[assembly_name]
        except KeyError as e:
            raise ValueError(f"Unable to find assembly {assembly_name}") from e

        upa_dir = os.path.join(output_dir, upa)
        os.makedirs(upa_dir, exist_ok=True)

        dest_file = os.path.join(upa_dir, assembly_name)
        loader_helper.create_hardlink_between_files(dest_file, src_file)


def upload_assemblies_to_workspace(
        conf: Conf,
        workspace_name: str,
        assembly_files: dict[str, str],
) -> list[str]:
    """
    Upload assemblies to workspace and record failed assembly paths.

    conf: Conf object
    workspace_name: Workspace addressed by the permanent ID
    assembly_files: a dictionary of assembly name maps to file path
    """

    print(f'start uploading {len(assembly_files)} assembly files')

    failed_paths = list()

    counter = 1
    for assembly_name, assembly_path in assembly_files.items():
        if counter % 5000 == 0:
            print(f"{round(counter / len(assembly_files), 4) * 100}% finished at {datetime.now()}")

        try:
            _upload_assembly_to_workspace(conf, workspace_name, assembly_path, assembly_name)
        except Exception as e:
            print(e)
            failed_paths.append(assembly_path)

        counter += 1

    if failed_paths:
        print(f'Failed to upload {failed_paths}')

    return failed_paths


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
    overwrite = args.overwrite

    env = args.env
    kb_base_url = loader_common_names.KB_BASE_URL_MAP[env]

    if workspace_id <= 0:
        parser.error(f"workspace_id needs to be > 0")

    uid = os.getuid()
    username = os.getlogin()

    job_dir = loader_helper.make_job_dir(
        root_dir, loader_common_names.SDK_JOB_DIR, username
    )
    csd = loader_helper.make_collection_source_dir(
        root_dir, loader_common_names.DEFAULT_ENV, kbase_collection, source_version
    )
    csd_upload = loader_helper.make_collection_source_dir(
        root_dir, loader_common_names.DEFAULT_ENV, kbase_collection, source_version, True
    )
    output_dir = loader_helper.make_output_dir(
        root_dir,
        loader_common_names.SOURCE_DATA_DIR,
        loader_common_names.WS,
        env,
        workspace_id,
    )

    proc = None
    conf = None

    try:
        # start podman service
        proc = loader_helper.start_podman_service(uid)
    except Exception as e:
        raise Exception("Podman service failed to start") from e
    else:
        # set up conf, start callback server, and upload assemblies to workspace
        conf = Conf(job_dir, kb_base_url, csd, token_filepath)
        workspace_name = conf.ws.get_workspace_info({"id": workspace_id})[1]
        assembly_objs = loader_helper.list_objects(
            workspace_id, conf, loader_common_names.OBJECTS_NAME_ASSEMBLY
        )
        uploaded_assembly_names = [obj[1] for obj in assembly_objs]
        all_assemblies, wait_to_upload_assemblies = _fetch_assemblies_to_upload(
            workspace_name,
            csd,
            uploaded_assembly_names,
            upload_file_ext,
            overwrite,
        )

        if not wait_to_upload_assemblies:
            print(f"All {len(all_assemblies)} assembly files already exist in workspace id: {workspace_id}")
            create_entries_in_sd_workspace(conf, workspace_id, all_assemblies, output_dir)
            loader_helper.create_softlinks_in_csd(csd_upload, output_dir, list(all_assemblies.keys()))
            return

        print(f"Originally planned to upload {len(all_assemblies)} assembly files")
        print(
            f"Will overwrite existing assembly files"
            if overwrite
            else f"Detected {len(all_assemblies) - len(wait_to_upload_assemblies)} assembly files already exist"
        )

        failed_paths = upload_assemblies_to_workspace(conf, workspace_name, wait_to_upload_assemblies)
        success_dict = {k: v for k, v in wait_to_upload_assemblies.items() if v not in failed_paths}

        if failed_paths:
            print(f"\nFailed to upload {failed_paths}")
        else:
            print(f"\nSuccessfully upload {len(wait_to_upload_assemblies)} assemblies")

        create_entries_in_sd_workspace(conf, workspace_id, success_dict, output_dir)
        loader_helper.create_softlinks_in_csd(csd_upload, output_dir, list(success_dict.keys()))

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
