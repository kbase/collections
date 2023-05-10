"""
usage: workspace_downloader.py [-h] --workspace_id WORKSPACE_ID [--kbase_collection KBASE_COLLECTION] [--source_version SOURCE_VERSION]
                               [--root_dir ROOT_DIR] [--kb_base_url KB_BASE_URL] [--workers WORKERS] [--token_filepath TOKEN_FILEPATH]
                               [--keep_job_dir]

PROTOTYPE - Download genome files from the workspace service (WSS).

options:
  -h, --help            show this help message and exit

required named arguments:
  --workspace_id WORKSPACE_ID
                        Workspace addressed by the permanent ID

optional arguments:
  --kbase_collection KBASE_COLLECTION
                        Create a collection and link in data to that collection from the overall workspace source data dir
  --source_version SOURCE_VERSION
                        Create a source version and link in data to that collection from the overall workspace source data dir
  --root_dir ROOT_DIR   Root directory. (default: /global/cfs/cdirs/kbase/collections)
  --kb_base_url KB_BASE_URL
                        KBase base URL, defaulting to prod (default: https://kbase.us/services/)
  --workers WORKERS     Number of workers for multiprocessing (default: 5)
  --token_filepath TOKEN_FILEPATH
                        A file path that stores KBase token
  --keep_job_dir        Keep SDK job directory after download task is completed

            
e.g.
PYTHONPATH=. python src/loaders/workspace_downloader/workspace_downloader.py --workspace_id 39795 --kbase_collection PMI --source_version 2023.1 --kb_base_url https://ci.kbase.us/services/ --keep_job_dir

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
import uuid
from multiprocessing import Pool, Queue, cpu_count

import docker

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_common_names, loader_helper

# setup KB_AUTH_TOKEN as env or provide a token_filepath in --token_filepath
# export KB_AUTH_TOKEN="your-kb-auth-token"

# supported source of data
SOURCE = "WS"
# filtering applied to list objects
FILTER_OBJECTS_NAME_BY = "KBaseGenomeAnnotations.Assembly"


class Conf:
    def __init__(
        self,
        job_dir,
        output_dir,
        workers,
        kb_base_url,
        token_filepath,
    ):
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
        self.queue = Queue()
        self.pth = output_dir
        self.job_dir = job_dir
        self.pools = Pool(workers, process_input, [self])

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


def _make_collection_source_dir(
    root_dir, collection_data_dir, collection, source_verion, source_data_dir
):
    """
    Helper function that create a collection & source_version and link in data
    to that colleciton from the overall workspace source data dir.
    """
    collection_source_dir = os.path.join(
        root_dir, collection_data_dir, collection, source_verion, source_data_dir
    )
    os.makedirs(collection_source_dir, exist_ok=True)
    return collection_source_dir


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
        f"--{loader_common_names.KBASE_COLLECTION_ARG_NAME}",
        type=str,
        help="Create a collection and link in data to that collection from the overall workspace source data dir",
    )
    optional.add_argument(
        "--source_version",
        type=str,
        help="Create a source version and link in data to that collection from the overall workspace source data dir",
    )
    optional.add_argument(
        "--root_dir",
        type=str,
        default=loader_common_names.ROOT_DIR,
        help="Root directory.",
    )
    optional.add_argument(
        "--kb_base_url",
        type=str,
        default=loader_common_names.KB_BASE_URL_DEFAULT,
        help="KBase base URL, defaulting to prod",
    )
    optional.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Number of workers for multiprocessing",
    )
    optional.add_argument(
        "--token_filepath",
        type=str,
        help="A file path that stores KBase token",
    )
    optional.add_argument(
        "--keep_job_dir",
        action="store_true",
        help="Keep SDK job directory after download task is completed",
    )

    args = parser.parse_args()

    workspace_id = args.workspace_id
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    source_version = args.source_version
    root_dir = args.root_dir
    kb_base_url = args.kb_base_url
    workers = args.workers
    token_filepath = args.token_filepath
    keep_job_dir = args.keep_job_dir

    if bool(kbase_collection) ^ bool(source_version):
        parser.error(
            f"if either kbase_collection or source_verion is specified, both are required"
        )
    if workspace_id <= 0:
        parser.error(f"workspace_id needs to be > 0")
    if workers < 1 or workers > cpu_count():
        parser.error(f"minimum worker is 1 and maximum worker is {cpu_count()}")

    uid = os.getuid()
    username = os.getlogin()

    job_dir = _make_job_dir(root_dir, loader_common_names.SDK_JOB_DIR, username)
    output_dir = _make_output_dir(
        root_dir, loader_common_names.SOURCE_DATA_DIR, SOURCE, workspace_id
    )
    collection_source_dir = None
    if kbase_collection:
        collection_source_dir = _make_collection_source_dir(
            root_dir,
            loader_common_names.COLLECTION_DATA_DIR,
            kbase_collection,
            source_version,
            loader_common_names.SOURCE_DATA_DIR,
        )

    proc = None
    conf = None

    try:
        # start podman service
        proc = loader_helper.start_podman_service(uid)
    except Exception as e:
        raise Exception("Podman service failed to start") from e
    else:
        # set up conf and start callback server
        conf = Conf(
            job_dir,
            output_dir,
            workers,
            kb_base_url,
            token_filepath,
        )
        objs = list_objects(workspace_id, conf, FILTER_OBJECTS_NAME_BY)
        upas = []

        for obj_info in objs:
            upa = "{6}_{0}_{4}".format(*obj_info)
            upas.append(upa)
            upa_dir = os.path.join(output_dir, upa)
            if os.path.isdir(upa_dir) and loader_helper.is_upa_info_complete(upa_dir):
                continue

            # remove legacy upa_dir to avoid FileExistsError in hard link
            if os.path.isdir(upa_dir):
                shutil.rmtree(upa_dir)
            conf.queue.put([upa, obj_info])

        for i in range(workers + 1):
            conf.queue.put(None)

        conf.pools.close()
        conf.pools.join()

        # create a softlink from the relevant directory under collectionsdata
        if collection_source_dir:
            for upa in upas:
                upa_dir = os.path.join(output_dir, upa)
                csd_upa_dir = os.path.join(collection_source_dir, upa)
                if os.path.isdir(csd_upa_dir):
                    # check if it is a symbolic link or regular dir
                    if os.path.islink(csd_upa_dir):
                        if os.readlink(csd_upa_dir) != upa_dir:
                            os.unlink(csd_upa_dir)
                    else:
                        shutil.rmtree(csd_upa_dir)
                os.symlink(upa_dir, csd_upa_dir, target_is_directory=True)

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
