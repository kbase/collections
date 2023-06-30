import argparse
import os
import shutil
import stat
import time
import uuid

import docker

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.loaders.common import loader_common_names, loader_helper

# setup KB_AUTH_TOKEN as env or provide a token_filepath in --token_filepath
# export KB_AUTH_TOKEN="your-kb-auth-token"

# supported source of data
SOURCE = "WS"

KB_BASE_URL_MAP = {'CI': 'https://ci.kbase.us/services/',
                   'NEXT': 'https://next.kbase.us/services/',
                   'APPDEV': 'https://appdev.kbase.us/services/',
                   'PROD': 'https://kbase.us/services/'}

class Conf:
    def __init__(self, job_dir, kb_base_url, token_filepath):
        port = loader_helper.find_free_port()
        token = loader_helper.get_token(token_filepath)
        self.start_callback_server(
            docker.from_env(), uuid.uuid4().hex, job_dir, kb_base_url, token, port
        )
        callback_url = "http://" + loader_helper.get_ip() + ":" + str(port)
        print("callback_url:", callback_url)
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
            image=loader_common_names.CALLBACK_IMAGE_NAME,
            detach=True,
            network_mode="host",
            environment=env,
            volumes=vol,
        )
        time.sleep(2)

    def stop_callback_server(self):
        self.container.stop()
        self.container.remove()


def _make_job_dir(root_dir, job_dir, username):
    """Helper function that creates a job_dir for a user under root directory."""
    job_dir = os.path.join(root_dir, job_dir, username)
    os.makedirs(job_dir, exist_ok=True)
    # only user can cread, write, or execute
    os.chmod(job_dir, stat.S_IRWXU)
    return job_dir


def main():
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
        default='PROD',
        help="KBase environment, defaulting to PROD",
    )
    optional.add_argument(
        "--keep_job_dir",
        action="store_true",
        help="Keep SDK job directory after download task is completed",
    )

    args = parser.parse_args()

    workspace_id = args.workspace_id
    root_dir = args.root_dir
    token_filepath = args.token_filepath
    keep_job_dir = args.keep_job_dir
    env = args.env

    kb_base_url = KB_BASE_URL_MAP[env]

    if workspace_id <= 0:
        parser.error(f"workspace_id needs to be > 0")
 

    uid = os.getuid()
    username = os.getlogin()

    job_dir = _make_job_dir(root_dir, loader_common_names.SDK_JOB_DIR, username)
 

    proc = None
    conf = None

    try:
        # start podman service
        proc = loader_helper.start_podman_service(uid)
    except Exception as e:
        raise Exception("Podman service failed to start") from e
    else:
        # set up conf and start callback server
        upa = "69036_370_1"
        worksapce_name = "sijiex:narrative_1688077625427"
        conf = Conf(job_dir, kb_base_url, token_filepath)
        fasta_file = conf.asu.get_assembly_as_fasta({"ref": upa.replace("_", "/"), "filename": upa})
        conf.asu.save_assembly_from_fasta(
            {"file": {"path": os.path.join(job_dir, "workdir", "tmp", upa)},
             "workspace_name": worksapce_name,
             "assembly_name": fasta_file['assembly_name']})

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
