"""
usage: workspace_downloader.py [-h] --workspace_id WORKSPACE_ID [--root_dir ROOT_DIR] [--kb_base_url KB_BASE_URL] [--workers {1,2,3,4}]
                               [--token_filename TOKEN_FILENAME] [--delete_job_dir]

PROTOTYPE - Download genome files from the workspace service (WSS).

options:
  -h, --help            show this help message and exit

required named arguments:
  --workspace_id WORKSPACE_ID
                        Workspace addressed by the permanent ID

optional arguments:
  --root_dir ROOT_DIR   Root directory.
  --kb_base_url KB_BASE_URL
                        KBase base URL, defaulting to prod
  --workers             Number of workers for multiprocessing
  --token_filename TOKEN_FILENAME
                        Filename in home directory that stores token
  --delete_job_dir      Delete job directory

            
e.g.
PYTHONPATH=. python src/loaders/workspace_downloader/workspace_downloader.py --workspace_id 39795 --kb_base_url https://ci.kbase.us/services/

NOTE:
NERSC file structure for WS:
/global/cfs/cdirs/kbase/collections/sourcedata/ -> WS -> workspace ID -> UPA -> .fa && .meta files 

e.g.
/global/cfs/cdirs/kbase/collections/sourcedata/WS -> 39795 -> 39795_10_1 -> 39795_10_1.fa 
                                                                         -> 39795_10_1.meta
                                                              39795_22_1 -> 39795_22_1.fa 
                                                                         -> 39795_22_1.meta
                                                     
"""
import argparse
import itertools
import json
import os
import shutil
import stat
import time
from multiprocessing import Pool, Queue, cpu_count

import docker

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_common_names, loader_helper

# setup KB_AUTH_TOKEN as env or provide a token_filename in --token_filename
# export KB_AUTH_TOKEN="your-kb-auth-token"

# supported source of data
SOURCE = "WS"
# filtering applied to list objects
FILTER_OBJECTS_NAME_BY = "KBaseGenomeAnnotations.Assembly"
# callback_port
CALLBACK_PORT = 9999


class Conf:
    def __init__(self, job_dir, output_dir, workers, kb_base_url, token_filename):
        self.setup_callback_server_envs(job_dir, kb_base_url, token_filename)
        self.start_callback_server(docker.from_env(), "cb")
        time.sleep(2)

        # os.environ['SDK_CALLBACK_URL'] = self.cb.callback_url
        token = os.environ["KB_AUTH_TOKEN"]
        ws_url = os.path.join(kb_base_url, "ws")
        callback_url = "http://" + loader_helper.get_ip() + ":" + str(CALLBACK_PORT)
        print("callback_url: ", callback_url)

        self.ws = Workspace(ws_url, token=token)
        self.asu = AssemblyUtil(callback_url, token=token)
        self.queue = Queue()
        self.pth = output_dir
        self.job_dir = job_dir
        self.pools = Pool(workers, process_input, [self])

    def setup_callback_server_envs(self, job_dir, kb_base_url, token_filename):
        # initiate env and vol
        self.env = {}
        self.vol = {}

        # setup envs required for docker container
        token = os.environ.get("KB_AUTH_TOKEN")
        if not token:
            if not token_filename:
                raise ValueError("Need to provide a token_filename")
            token = loader_helper.get_token(token_filename)
        os.environ["KB_AUTH_TOKEN"] = token

        self.env["KB_AUTH_TOKEN"] = token
        # used by the callback server
        self.env["KB_BASE_URL"] = kb_base_url
        # used by the callback server
        self.env["JOB_DIR"] = job_dir
        # used by the callback server
        self.env["CALLBACK_PORT"] = CALLBACK_PORT

        # setup volumes required for docker container
        docker_host = os.environ["DOCKER_HOST"]
        if docker_host.startswith("unix:"):
            docker_host = docker_host[5:]
        self.vol[job_dir] = {"bind": job_dir, "mode": "rw"}
        self.vol[docker_host] = {"bind": "/run/docker.sock", "mode": "rw"}

    def start_callback_server(self, client, container_name):
        container = None
        try:
            container = client.containers.get(container_name)
        except Exception as e:
            print("container does not exist and will fetch scanon/callback ")
        status = container.attrs["State"]["Status"] if container else None
        print("self.env: ", self.env)
        print("self.vol: ", self.vol)
        if not status:
            container = client.containers.run(
                name=container_name,
                image="scanon/callback",
                detach=True,
                network_mode="host",
                environment=self.env,
                volumes=self.vol,
            )
        elif status == "existed":
            container.start()
        elif status == "paused":
            container.unpause()
        else:
            print("container is restarting or running ...")
        self.container = container

    def stop_callback_server(self):
        self.container.stop()
        self.container.remove()


def _make_output_dir(root_dir, source_data_dir, source, workspace_id):
    """Helper function that makes output directory for a specific collection under root directory."""
    output_dir = os.path.join(root_dir, source_data_dir, source, str(workspace_id))
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def _make_job_dir(root_dir, job_dir, username):
    """Helper function that create a job_dir for a user under root directory."""
    job_dir = os.path.join(root_dir, job_dir, username)
    os.makedirs(job_dir, exist_ok=True)
    # only user can cread, write, or execute
    os.chmod(job_dir, stat.S_IRWXU)
    return job_dir


def _list_objects_params(wsid, min_id, max_id, type_str):
    """Helper function that creats params needed for list_objects function."""
    params = {
        "ids": [wsid],
        "minObjectID": min_id,
        "maxObjectID": max_id,
        "type": type_str,
    }
    return params


def _process_object_info(obj_info):
    """
    "upa", "name", "type", and "timestamp info will be extracted from object info and save as a dict."
    {
        "upa": "790541/67/2",
        "name": <copy object name from object info>
        "type": <copy object type from object info>
        "timestamp": <copy timestamp from object info>
    }
    """
    res_dict = {}
    res_dict["upa"] = "{6}/{0}/{4}".format(*obj_info)
    res_dict["name"] = obj_info[1]
    res_dict["type"] = obj_info[2]
    res_dict["timestamp"] = obj_info[3]
    return res_dict


def list_objects(wsid, conf, filter_objects_name_by, batch_size=10000):
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
            _list_objects_params(wsid, min_id, max_id, filter_objects_name_by)
        )
        for min_id, max_id in batch_input
    ]
    res_objs = list(itertools.chain.from_iterable(objs))
    return res_objs


def process_input(conf):
    """
    Download .fa and .meta files from workspace and save a copy under output_dir.
    """
    while True:
        task = conf.queue.get(block=True)
        if not task:
            print("Stopping")
            break
        upa, obj_info = task

        # cfn points to the assembly file outside of the container
        # get_assembly_as_fasta writes the file to /kb/module/workdir/tmp/<filename> inside the container.
        # workdir is shared between the container and the external file system
        # Any file path get_assembly_as_fasta returns will be relative to inside the container, and so is not useful for this script

        cfn = os.path.join(conf.job_dir, "workdir/tmp", upa)
        # upa file is downloaded to cfn
        conf.asu.get_assembly_as_fasta({"ref": upa.replace("_", "/"), "filename": upa})

        # each upa in output_dir as a seperate directory
        dstd = os.path.join(conf.pth, upa)
        os.makedirs(dstd, exist_ok=True)

        dst = os.path.join(dstd, f"{upa}.fa")
        # Hard link .fa file from job_dir to output_dir in WS
        os.link(cfn, dst)

        metafile = os.path.join(dstd, f"{upa}.meta")
        # save meta file with relevant object_info
        with open(metafile, "w", encoding="utf8") as json_file:
            json.dump(_process_object_info(obj_info), json_file, indent=2)

        print("Completed %s" % (upa))


def main():
    parser = argparse.ArgumentParser(
        description="PROTOTYPE - Download genome files from the workspace service (WSS)."
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
        "--kb_base_url",
        type=str,
        default=loader_common_names.KB_BASE_URL,
        help="KBase base URL, defaulting to prod",
    )
    optional.add_argument(
        "--workers",
        type=int,
        default=5,
        choices=range(1, cpu_count() + 1),
        help="Number of workers for multiprocessing",
    )
    optional.add_argument(
        "--token_filename",
        type=str,
        help="Filename in home directory that stores token",
    )
    optional.add_argument(
        "--delete_job_dir",
        action="store_true",
        help="Delete job directory",
    )

    args = parser.parse_args()

    (workspace_id, root_dir, kb_base_url, workers, token_filename, delete_job_dir) = (
        args.workspace_id,
        args.root_dir,
        args.kb_base_url,
        args.workers,
        args.token_filename,
        args.delete_job_dir,
    )

    uid = os.getuid()
    username = os.getlogin()

    job_dir = _make_job_dir(root_dir, loader_common_names.JOB_DIR, username)
    output_dir = _make_output_dir(
        root_dir, loader_common_names.SOURCE_DATA_DIR, SOURCE, workspace_id
    )
    boolean_flag = False
    try:
        # start podman service
        proc = loader_helper.start_podman_service(uid)
        boolean_flag = True
    except:
        raise Exception("Podman service fails to start")
    else:
        # set up conf and start callback server
        conf = Conf(job_dir, output_dir, workers, kb_base_url, token_filename)

        visited = set(
            [
                upa
                for upa in os.listdir(output_dir)
                if loader_helper.is_upa_info_complete(output_dir, upa)
            ]
        )
        print("Skipping: ", visited)

        for obj_info in list_objects(workspace_id, conf, FILTER_OBJECTS_NAME_BY):
            upa = "{6}_{0}_{4}".format(*obj_info)
            if upa in visited:
                continue
            # remove legacy upa_dir to avoid FileExistsError in hard link
            dirpath = os.path.join(output_dir, upa)
            if os.path.exists(dirpath) and os.path.isdir(dirpath):
                shutil.rmtree(dirpath)
            conf.queue.put([upa, obj_info])

        for i in range(workers + 1):
            conf.queue.put(None)

        conf.pools.close()
        conf.pools.join()
        conf.stop_callback_server()

    finally:
        # stop podman service if is on
        if boolean_flag:
            proc.terminate()

    if delete_job_dir:
        shutil.rmtree(job_dir)


if __name__ == "__main__":
    main()
