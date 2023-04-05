"""
usage: workspace_downloader.py [-h] --workspace_id WORKSPACE_ID [--project_dir PROJECT_DIR] [--root_dir ROOT_DIR] [--output_dir OUTPUT_DIR]
                               [--job_dir JOB_DIR] [--source SOURCE] [--workers WORKERS] [--overwrite] [--batch_size BATCH_SIZE]

PROTOTYPE - Download genome files from the workspace service (WSS).

optional arguments:
  -h, --help            show this help message and exit

required named arguments:
  --workspace_id WORKSPACE_ID
                        Workspace addressed by the permanent ID
  --project_dir PROJECT_DIR
                        Path points to Collections repository

optional arguments:
  --root_dir ROOT_DIR   Root directory.
  --output_dir OUTPUT_DIR
                        Output directoy to save genome objects.
  --job_dir JOB_DIR     Job directoy of source link
  --source SOURCE       Source of data (default: WS)
  --workers WORKERS     Number of workers for multiprocessing
  --overwrite           Overwrite existing files.
  --batch_size BATCH_SIZE
                        Batch size of object id

                        
e.g.
PYTHONPATH=. python src/loaders/workspace_downloader/workspace_downloader.py --workspace_id 39795 --project_dir /global/homes/s/sijiex/collections

NOTE:
NERSC file structure for WS:
/global/cfs/cdirs/kbase/collections/sourcedata/ -> WS -> UPA -> .fa && .meta files 

e.g.
/global/cfs/cdirs/kbase/collections/sourcedata/WS -> 39795_10_1 -> 39795_10_1.fa 
                                                                -> 39795_10_1.meta
                                                     39795_22_1 -> 39795_22_1.fa 
                                                                -> 39795_22_1.meta
                                                     
"""
import argparse
import itertools
import json
import os
import shutil
from subprocess import CalledProcessError, Popen, check_output
import time
from multiprocessing import Pool, Queue

from JobRunner.Callback import Callback

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_common_names, loader_helper


SOURCE = "WS"  # supported source of data
WS_DOMAIN = "https://ci.kbase.us/services/ws"  # workspace link
FILTER_OBJECTS_NAME_BY = (
    "KBaseGenomeAnnotations.Assembly"  # filtering applied to list objects
)
TOKEN_PATH = "~/.kbase_ci"  # token path


class Service:
    def __init__(self, uid, job_dir):
        self.id = uid
        self.job_dir = job_dir

    def start(self):
        # Set the DOCKER_HOST
        os.environ["DOCKER_HOST"] = loader_common_names.DOCKER_HOST.format(self.id)

        # Provide your token path or default token path ~/.kbase_ci or ~/.kbase_prod will be used
        os.environ["KB_AUTH_TOKEN"] = loader_helper.store_token(TOKEN_PATH)

        # Set the base url if not using prod
        os.environ["KB_BASE_URL"] = loader_common_names.KB_BASE_URL

        # Set the JOB_DIR
        os.environ["JOB_DIR"] = self.job_dir

    # def stop(self):
    #     loader_helper.stop_podman()


class Conf:
    def __init__(self, job_dir, output_dir, workers):
        self.cb = Callback()
        self.cb.start()
        time.sleep(2)
        # os.environ['SDK_CALLBACK_URL'] = self.cb.callback_url
        token = os.environ["KB_AUTH_TOKEN"]
        self.ws = Workspace(WS_DOMAIN, token=token)
        self.asu = AssemblyUtil(self.cb.callback_url, token=token)
        self.queue = Queue()
        self.pth = output_dir
        self.job_dir = job_dir
        self.pools = Pool(workers, process_input, [self])


def _start_podman_service():
    """Helper function that will start podman if not already running"""
    try:
        check_output(["pidof", "podman"])
    except CalledProcessError:
        print("No running podmans servies are detected. Start one now!")
        proc = Popen(["podman", "system", "service", "-t", "0"])
        return proc


def _make_output_dir(root_dir, source_data_dir, source):
    """Helper function that makes working directory for a specific collection under root directory"""

    if source == "WS":
        return os.path.join(root_dir, source_data_dir, source)
    raise ValueError(f"Unexpected source: {source}")


def _make_job_dir(project_dir, username):
    """Helper function that create a job dir if not provided by the user"""
    return os.path.join(project_dir, username)


def _list_objects_params(wsid, min_id, max_id):
    """Helper function that creats params needed for list_objects function"""
    params = {"ids": [wsid], "minObjectID": min_id, "maxObjectID": max_id}
    return params


def _process_object_info(obj_info):
    """
    "upa", "name", "type", and "timestamp info will be extracted from object info and save as a dict"
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


def list_objects(wsid, conf, batch_size=10000, filter_objects_name_by=None):
    """
    List all objects information given a workspace ID
    """
    if batch_size > 10000:
        raise ValueError("Maximum value for listing workspace objects is 10000")

    maxObjectID = conf.ws.get_workspace_info({"id": wsid})[4]
    batch_input = [
        [idx + 1, idx + batch_size] for idx in range(0, maxObjectID, batch_size)
    ]
    objs = [
        conf.ws.list_objects(_list_objects_params(wsid, min_id, max_id))
        for min_id, max_id in batch_input
    ]
    res_objs = list(itertools.chain.from_iterable(objs))
    if filter_objects_name_by:
        res_objs = [
            obj for obj in res_objs if obj[2].startswith(filter_objects_name_by)
        ]
    return res_objs


def process_input(conf):
    """
    Download .fa and .meta files from workspace and save a copy under output_dir
    """
    while True:
        task = conf.queue.get(block=True)
        if not task:
            print("Stopping")
            break
        upa, obj_info = task
        cfn = os.path.join(conf.job_dir, "workdir/tmp", upa)
        # upa file is downloaded to cfn
        conf.asu.get_assembly_as_fasta({"ref": upa.replace("_", "/"), "filename": upa})

        # each upa in output_dir as a seperate directory
        dstd = os.path.join(conf.pth, upa)
        os.makedirs(dstd, exist_ok=True)

        dst = os.path.join(dstd, f"{upa}.fa")
        # copy downloaded upa file to output_dir. Hard link might cause invalid cross-device link problem
        shutil.copy(cfn, dst)

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
    required.add_argument(
        "--project_dir",
        type=str,
        help="Path points to Collections repository",
    )

    # Optional argument
    optional.add_argument(
        "--root_dir",
        type=str,
        default=loader_common_names.ROOT_DIR,
        help="Root directory.",
    )
    optional.add_argument(
        "--output_dir",
        type=str,
        help="Output directoy to save genome objects.",
    )
    optional.add_argument(
        "--job_dir",
        type=str,
        help="Job directoy of source link",
    )
    optional.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Number of workers for multiprocessing",
    )
    optional.add_argument(
        "--overwrite", action="store_true", help="Overwrite existing files."
    )

    args = parser.parse_args()

    (
        workspace_id,
        project_dir,
        root_dir,
        output_dir,
        job_dir,
        workers,
        overwrite,
    ) = (
        args.workspace_id,
        args.project_dir,
        args.root_dir,
        args.output_dir,
        args.job_dir,
        args.workers,
        args.overwrite,
    )

    proc = _start_podman_service()

    uid = loader_helper.get_id()
    username = loader_helper.get_username()

    job_dir = job_dir or _make_job_dir(project_dir, username)
    output_dir = output_dir or _make_output_dir(
        root_dir, loader_common_names.SOURCE_DATA_DIR, SOURCE
    )

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(job_dir, exist_ok=True)

    service = Service(uid, job_dir)
    service.start()

    conf = Conf(job_dir, output_dir, workers)
    visited = set(os.listdir(output_dir))

    for obj_info in list_objects(
        workspace_id, conf, filter_objects_name_by=FILTER_OBJECTS_NAME_BY
    ):
        upa = "{6}_{0}_{4}".format(*obj_info)
        if upa in visited and not overwrite:
            raise ValueError(
                "{} is already in {}. Please add --overwrite flag to redownload".format(
                    upa, output_dir
                )
            )
        conf.queue.put([upa, obj_info])

    for i in range(workers + 1):
        conf.queue.put(None)

    conf.pools.close()
    conf.pools.join()
    conf.cb.stop()
    if proc:
        proc.terminate()


if __name__ == "__main__":
    main()
