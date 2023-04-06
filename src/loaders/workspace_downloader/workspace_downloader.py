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
import time
from multiprocessing import Pool, Queue

from JobRunner.Callback import Callback

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_common_names, loader_helper

SOURCE = "WS"  # supported source of data
FILTER_OBJECTS_NAME_BY = (
    "KBaseGenomeAnnotations.Assembly"  # filtering applied to list objects
)


class Conf:
    def __init__(self, job_dir, output_dir, workers, ws_domain):
        self.cb = Callback()
        self.cb.start()
        time.sleep(2)
        # os.environ['SDK_CALLBACK_URL'] = self.cb.callback_url
        token = os.environ["KB_AUTH_TOKEN"]
        self.ws = Workspace(ws_domain, token=token)
        self.asu = AssemblyUtil(self.cb.callback_url, token=token)
        self.queue = Queue()
        self.pth = output_dir
        self.job_dir = job_dir
        self.pools = Pool(workers, process_input, [self])


def positive_number(value):
    """Checks if input is positive number and returns value as an int type."""
    if not isinstance(value, (str, int)):
        raise argparse.ArgumentTypeError(
            f"Input must be an integer or string type, you have specified '{value}' which is of type {type(value)}"
        )

    try:
        int_val = int(value)
    except ValueError:
        raise ValueError(f"Unable to convert {value} to int")

    if int_val <= 0:
        raise argparse.ArgumentTypeError(
            f"Input: {value} converted to int: {int_val} must be a positive number"
        )
    return int_val


def _make_output_dir(root_dir, source_data_dir, source):
    """Helper function that makes output directory for a specific collection under root directory"""

    if source == "WS":
        output_dir = os.path.join(root_dir, source_data_dir, source)
    else:
        raise ValueError(f"Unexpected source: {source}")

    os.makedirs(output_dir, exist_ok=True)

    return output_dir


def _make_job_dir(root_dir, job_dir, username):
    """Helper function that create a job_dir for a user under root directory"""
    job_dir = os.path.join(root_dir, job_dir, username)
    os.makedirs(job_dir, exist_ok=True)
    return job_dir


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
        "--workers",
        type=positive_number,
        default=5,
        help="Number of workers for multiprocessing",
    )
    optional.add_argument(
        "--overwrite", action="store_true", help="Overwrite existing files."
    )
    optional.add_argument(
        "--token_filename",
        type=str,
        default="kbase_prod",
        help="Filename in home directory that stores token",
    )
    optional.add_argument(
        "--ci",
        action="store_true",
        help="Use ci env. Default to prod",
    )
    optional.add_argument(
        "--delete_job_dir",
        action="store_true",
        help="Delete job directory",
    )

    args = parser.parse_args()

    (workspace_id, root_dir, workers, overwrite, token_filename, ci, delete_job_dir) = (
        args.workspace_id,
        args.root_dir,
        args.workers,
        args.overwrite,
        args.token_filename,
        args.ci,
        args.delete_job_dir,
    )

    uid = os.getuid()
    username = os.getlogin()

    KB_BASE_URL = loader_common_names.KB_BASE_URL
    if ci:
        KB_BASE_URL = KB_BASE_URL[:8] + "ci." + KB_BASE_URL[8:]
    WS_DOMAIN = os.path.join(KB_BASE_URL, "ws")  # workspace link

    job_dir = _make_job_dir(root_dir, loader_common_names.JOB_DIR, username)
    output_dir = _make_output_dir(root_dir, loader_common_names.SOURCE_DATA_DIR, SOURCE)

    # start podman service
    proc = loader_helper.start_podman_service()

    # Used by the podman service
    os.environ["DOCKER_HOST"] = loader_common_names.DOCKER_HOST.format(uid)

    # used by the callback server
    if not os.environ.get("KB_AUTH_TOKEN"):
        os.environ["KB_AUTH_TOKEN"] = loader_helper.get_token(token_filename)

    # used by the callback server
    os.environ["KB_BASE_URL"] = KB_BASE_URL

    # used by the callback server
    os.environ["JOB_DIR"] = job_dir

    conf = Conf(job_dir, output_dir, workers, WS_DOMAIN)
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

    # stop podman service
    proc.terminate()

    if delete_job_dir:
        shutil.rmtree(job_dir)


if __name__ == "__main__":
    main()
