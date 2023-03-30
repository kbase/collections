import argparse
import itertools
import json
import os
import subprocess
import time
from multiprocessing import Pool, Queue, cpu_count

from JobRunner.Callback import Callback
from utils.AssemblyUtilClient import AssemblyUtil
from utils.workspaceClient import Workspace

from src.loaders.common import loader_common_names

# Fraction amount of system cores can be utilized
# (i.e. 0.5 - program will use 50% of total processors,
#       0.1 - program will use 10% of total processors)
SYSTEM_UTILIZATION = 0.5
SOURCE = "WS"  # supported source of data
WS_DOMAIN = "https://ci.kbase.us/services/ws"
FILTER_OBJECTS_NAME_BY = "KBaseGenomeAnnotations.Assembly"


class Conf:
    def __init__(self, job_dir, output_dir, workers=5):
        self.cb = Callback()
        self.cb.start()
        time.sleep(2)
        # os.environ['SDK_CALLBACK_URL'] = self.cb.callback_url
        token = os.environ["KB_AUTH_TOKEN"]
        self.ws = Workspace(WS_DOMAIN, token=token)
        self.asu = AssemblyUtil(self.cb.callback_url, token=token)
        self.queue = Queue()
        self.workdir = self.cb.conf.workdir
        self.pth = output_dir
        self.job_dir = job_dir
        self.pools = Pool(workers, process_input, [self])


def _make_output_dir(root_dir, source_data_dir, source):
    # make working directory for a specific collection under root directory

    if source == "WS":
        work_dir = os.path.join(root_dir, source_data_dir, source)
    else:
        raise ValueError(f"Unexpected source: {source}")

    os.makedirs(work_dir, exist_ok=True)

    return work_dir


def _make_job_dir(project_dir):
    username = (
        subprocess.check_output("id", shell=True)
        .decode()
        .split(" ")[0]
        .split("(")[-1][:-1]
    )
    job_dir = os.path.join(project_dir, username)
    return job_dir


def _list_objects_params(wsid, min_id, max_id):
    params = {"ids": [wsid], "minObjectID": min_id, "maxObjectID": max_id}
    return params


def _process_object_info(obj_info):
    """
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


def list_objects(wsid, conf, batch_size, filter_objects_name_by=None):
    """
    list objects
    """

    maxObjectID = conf.ws.get_workspace_info({"id": wsid})
    batch_input = [
        [idx + 1, idx + batch_size] for idx in range(0, maxObjectID, batch_size)
    ]
    objs = [
        conf.ws.list_objects(_list_objects_params(min_id, max_id))
        for min_id, max_id in batch_input
    ]
    res_objs = list(itertools.chain.from_iterable(objs))
    if filter_objects_name_by:
        res_objs = [
            obj for obj in res_objs if obj[2].startswith(filter_objects_name_by)
        ]
    return res_objs


def process_input(conf):
    while True:
        upa, obj_info = conf.queue.get(block=True)
        if not upa:
            print("Stopping")
            break
        cfn = os.path.join(conf.job_dir, "workdir/tmp", upa)
        conf.asu.get_assembly_as_fasta({"ref": upa, "filename": upa})

        dstd = os.path.join(conf.pth, upa)
        os.makedirs(dstd, exist_ok=True)

        dst = os.path.join(dstd, f"{upa}.fa")
        # hark link source dir to destination .fa file only
        os.link(cfn, dst)

        metafile = os.path.join(dstd, f"{upa}.meta")
        json.dump(_process_object_info(obj_info), open(metafile, "w"), indent=2)

        print("Completed %s" % (upa))


def main():
    parser = argparse.ArgumentParser(
        description="PROTOTYPE - Download genome files from the workspace service (WSS)."
    )

    required = parser.add_argument_group("required named arguments")
    optional = parser.add_argument_group("optional arguments")

    # Required flag argument
    required.add_argument(
        "--worksapce_id",
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
        "--source", type=str, default="WS", help="Source of data (default: WS)"
    )
    optional.add_argument(
        "--threads",
        type=int,
        help="Number of threads. (default: half of system cpu count)",
    )
    optional.add_argument(
        "--overwrite", action="store_true", help="Overwrite existing files."
    )
    optional.add_argument(
        "--batch_size", type=int, default=10000, help="Batch size of object id"
    )

    args = parser.parse_args()

    (
        worksapce_id,
        project_dir, 
        root_dir,
        output_dir,
        job_dir,
        source,
        threads,
        overwrite,
        batch_size,
    ) = (
        args.worksapce_id,
        args.project_dir,
        args.root_dir,
        args.output_dir,
        args.job_dir,
        args.source,
        args.threads,
        args.overwrite,
        args.batch_size,
    )

    if source not in SOURCE:
        raise ValueError(f"Unexpected source. Currently supported sources: {SOURCE}")

    output_dir = output_dir or _make_output_dir(
        root_dir, loader_common_names.SOURCE_DATA_DIR, source
    )
    job_dir = job_dir or _make_job_dir(project_dir)

    if not threads:
        threads = max(int(cpu_count() * min(SYSTEM_UTILIZATION, 1)), 1)
    threads = max(1, threads)
    print(f"Start downloading genome files from workspace with {threads} threads")

    conf = Conf(job_dir, output_dir, threads)

    visited = set()
    for upa in os.listdir(output_dir):
        visited.add(upa)

    for obj_info in list_objects(
        worksapce_id, conf, batch_size, FILTER_OBJECTS_NAME_BY
    ):
        upa = "{6}/{0}/{4}".format(*obj_info)
        if upa in visited and not overwrite:
            raise ValueError(
                "{} is already in {}. Please add --overwrite flag to redownload".format(
                    upa, output_dir
                )
            )
        conf.queue.put([upa, obj_info])

    for i in range(threads + 1):
        conf.queue.put([None, None])

    conf.pools.close()
    conf.pools.join()
    conf.cb.stop()


if __name__ == "__main__":
    main()
