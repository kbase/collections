"""
usage: workspace_downloader.py [-h] --workspace_id WORKSPACE_ID [--kbase_collection KBASE_COLLECTION] [--source_version SOURCE_VERSION]
                               [--root_dir ROOT_DIR] [--kb_base_url KB_BASE_URL] [--workers WORKERS] [--token_filepath TOKEN_FILEPATH]
                               [--keep_job_dir] [--retrieve_sample] [--ignore_no_sample_error]

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
  --retrieve_sample     Retrieve sample for each genome object
  --ignore_no_sample_error
                        Ignore error when no sample data is found for an object

            
e.g.
PYTHONPATH=. python src/loaders/workspace_downloader/workspace_downloader.py --workspace_id 39795 --kbase_collection PMI --source_version 2023.01 --kb_base_url https://ci.kbase.us/services/ --keep_job_dir

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
from collections import defaultdict
from multiprocessing import Pool, Queue, cpu_count
from pathlib import Path
from typing import Any

import docker

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.SampleServiceClient import SampleService
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_common_names, loader_helper

# setup KB_AUTH_TOKEN as env or provide a token_filepath in --token_filepath
# export KB_AUTH_TOKEN="your-kb-auth-token"

# supported source of data
SOURCE = "WS"
# filename that logs genome duplicates for each assembly
GENOME_DUPLICATE_FILE = "duplicate_genomes.json"


class BadNodeTreeError(Exception):
    pass


class Conf:
    def __init__(
            self,
            job_dir,
            output_dir,
            workers,
            kb_base_url,
            token_filepath,
            retrieve_sample,
            ignore_no_sample_error
    ):
        port = loader_helper.find_free_port()
        self.token = loader_helper.get_token(token_filepath)
        self.retrieve_sample = retrieve_sample
        self.ignore_no_sample_error = ignore_no_sample_error
        self.start_callback_server(
            docker.from_env(), uuid.uuid4().hex, job_dir, kb_base_url, self.token, port
        )

        ws_url = os.path.join(kb_base_url, "ws")
        self.sample_url = os.path.join(kb_base_url, "sampleservice")
        callback_url = "http://" + loader_helper.get_ip() + ":" + str(port)
        print("callback_url:", callback_url)

        self.ws = Workspace(ws_url, token=self.token)
        self.asu = AssemblyUtil(callback_url, token=self.token)
        self.ss = SampleService(self.sample_url, token=self.token)
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
    """Helper function that creates a job_dir for a user under root directory."""
    job_dir = os.path.join(root_dir, job_dir, username)
    os.makedirs(job_dir, exist_ok=True)
    # only user can cread, write, or execute
    os.chmod(job_dir, stat.S_IRWXU)
    return job_dir


def _make_collection_source_dir(
        root_dir, collection_source_dir, collection, source_verion
):
    """
    Helper function that creates a collection & source_version and link in data
    to that colleciton from the overall workspace source data dir.
    """
    csd = os.path.join(root_dir, collection_source_dir, collection, source_verion)
    os.makedirs(csd, exist_ok=True)
    return csd


def _list_objects_params(wsid, min_id, max_id, type_str, include_metadata):
    """Helper function that creates params needed for list_objects function."""
    params = {
        "ids": [wsid],
        "minObjectID": min_id,
        "maxObjectID": max_id,
        "type": type_str,
        "includeMetadata": int(include_metadata),
    }
    return params


def _assembly_genome_lookup(genome_objs):
    """Helper function that creates a hashmap for the genome and its assembly reference"""
    hashmap = {}
    duplicate = defaultdict(list)
    for obj_info in genome_objs:
        genome_upa = "{6}/{0}/{4}".format(*obj_info)
        save_date = obj_info[3]
        try:
            assembly_upa = obj_info[-1]["Assembly Object"]
        except (TypeError, KeyError) as e:
            raise ValueError(
                f"Unbale to find 'Assembly Object' from {genome_upa}'s metadata"
            ) from e

        if not assembly_upa:
            raise ValueError(f"{genome_upa} does not have an assembly reference")

        genome_old = hashmap.get(assembly_upa)
        id_and_date = [genome_upa, save_date]
        if not genome_old:
            hashmap[assembly_upa] = id_and_date
        elif genome_old[1] > save_date:
            duplicate[assembly_upa].append(id_and_date)
        else:
            duplicate[assembly_upa].append(genome_old)
            hashmap[assembly_upa] = [id_and_date]

    # keep only genome_upa as value
    for assembly_upa in hashmap:
        hashmap[assembly_upa] = hashmap[assembly_upa][0]

    return hashmap, duplicate


def _create_softlink(csd_upa_dir, upa_dir):
    """
    Helper function that creates a softlink between two directories.
    """
    if os.path.exists(csd_upa_dir):
        if (
                os.path.isdir(csd_upa_dir)
                and os.path.islink(csd_upa_dir)
                and os.readlink(csd_upa_dir) == upa_dir
        ):
            return
        raise ValueError(
            f"{csd_upa_dir} already exists and does not link to {upa_dir} as expected"
        )
    os.symlink(upa_dir, csd_upa_dir, target_is_directory=True)


def _process_object_info(obj_info, genome_upa):
    """
    "upa", "name", "type", and "timestamp info will be extracted from object info and save as a dict."
    {
        "upa": "68981/9/1"
        "name": <copy object name from object info>
        "type": <copy object type from object info>
        "timestamp": <copy timestamp from object info>
        "genome_upa": "68981/507/1"
    }
    """
    res_dict = {}
    res_dict["upa"] = "{6}/{0}/{4}".format(*obj_info)
    res_dict["name"] = obj_info[1]
    res_dict["type"] = obj_info[2]
    res_dict["timestamp"] = obj_info[3]
    res_dict["genome_upa"] = genome_upa
    return res_dict


def list_objects(
        wsid, conf, filter_objects_name_by, include_metadata=False, batch_size=10000
):
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
            _list_objects_params(
                wsid, min_id, max_id, filter_objects_name_by, include_metadata
            )
        )
        for min_id, max_id in batch_input
    ]
    res_objs = list(itertools.chain.from_iterable(objs))
    return res_objs


def process_input(conf: Conf):
    """
    Download .fa and .meta files from workspace and save a copy under output_dir.
    """
    while True:
        task = conf.queue.get(block=True)
        if not task:
            print("Stopping")
            break
        upa, obj_info, genome_upa = task

        upa_dir = os.path.join(conf.pth, upa)
        metafile = os.path.join(upa_dir, f"{upa}.meta")
        if not os.path.isdir(upa_dir) or not loader_helper.is_upa_info_complete(upa_dir):

            # remove legacy upa_dir to avoid FileExistsError in hard link
            if os.path.isdir(upa_dir):
                shutil.rmtree(upa_dir)

            # cfn points to the assembly file outside of the container
            # get_assembly_as_fasta writes the file to /kb/module/workdir/tmp/<filename> inside the container.
            # workdir is shared between the container and the external file system
            # Any file path get_assembly_as_fasta returns will be relative to inside the container, and so is not useful for this script
            cfn = os.path.join(conf.job_dir, "workdir/tmp", upa)
            # upa file is downloaded to cfn
            conf.asu.get_assembly_as_fasta({"ref": upa.replace("_", "/"), "filename": upa})

            # each upa in output_dir as a separate directory
            os.makedirs(upa_dir, exist_ok=True)

            dst = os.path.join(upa_dir, f"{upa}.fa")
            # Hard link .fa file from job_dir to output_dir in WS
            os.link(cfn, dst)

            # save meta file with relevant object_info
            with open(metafile, "w", encoding="utf8") as json_file:
                json.dump(_process_object_info(obj_info, genome_upa), json_file, indent=2)

            print("Completed %s" % (upa))
        else:
            print(f"Skip downloading {upa} as it already exists")

        if conf.retrieve_sample:
            _download_sample_data(conf, genome_upa.replace("/", "_"), metafile)


def _download_sample_data(conf: Conf, upa: str, metafile: str) -> None:
    # retrieve sample data from sample service and save to file
    # additionally, retrieve node data from the sample data and save it to a file
    # NOTE: upa is in the format of "A_B_C"

    with open(metafile, "r", encoding="utf8") as json_file:
        meta = json.load(json_file)

    upa_dir = Path(metafile).parent
    sample_file_name = f"{upa}.{loader_common_names.SAMPLE_FILE_EXT}"
    sample_file = os.path.join(upa_dir, sample_file_name)
    if _check_file_exists(loader_common_names.SAMPLE_FILE_KEY, meta, sample_file):
        print(f"Skip downloading sample for {upa} as it already exists")
        return

    sample_ret = _retrieve_sample(conf, upa)
    if not sample_ret:
        return

    with open(sample_file, "w", encoding="utf8") as json_file:
        json.dump(sample_ret, json_file, indent=2)

    sample_prepared_name = f"{upa}.{loader_common_names.SAMPLE_PREPARED_EXT}"
    sample_prepared_file = os.path.join(upa_dir, sample_prepared_name)
    if _check_file_exists(loader_common_names.SAMPLE_PREPARED_KEY, meta, sample_prepared_file):
        print(f"Skip generating sample node tree for {upa} as it already exists")
        return

    try:
        node_data = _retrieve_node_data(sample_ret['node_tree'])
    except BadNodeTreeError as e:
        raise ValueError(f"Error retrieving node data for {upa}") from e

    with open(sample_prepared_file, "w", encoding="utf8") as json_file:
        json.dump(node_data, json_file, indent=2)

    # write sample file and prepared sample node file name back to the meta file
    meta[loader_common_names.SAMPLE_FILE_KEY] = sample_file_name
    meta[loader_common_names.SAMPLE_PREPARED_KEY] = sample_prepared_name
    with open(metafile, "w", encoding="utf8") as json_file:
        json.dump(meta, json_file, indent=2)


def _check_file_exists(
        file_key_name: str,
        metadata: dict[str, Any],
        file_path: str
) -> bool:
    # check if file exists and if the metadata matches the file name

    return (file_key_name in metadata and
            metadata[file_key_name] == Path(file_path).name and
            os.path.isfile(file_path))


def _retrieve_sample(conf: Conf, upa: str) -> dict[str, Any] | None:
    # retrieve sample data from sample service

    # retrieve data links associated with upa
    links_ret = conf.ss.get_data_links_from_data({"upa": upa.replace("_", "/")})

    data_links = links_ret['links']
    if not data_links and conf.ignore_no_sample_error:
        print(f"No sample data links found for {upa}")
        return

    # there should only be one data link for each upa
    if len(data_links) != 1:
        raise ValueError(f"Expected 1 data link for {upa}, got {len(data_links)}")

    # retrieve sample data and save to file
    sample_id = data_links[0]['id']
    sample_ret = conf.ss.get_sample_via_data({"upa": upa.replace("_", "/"),
                                              "id": sample_id,
                                              "version": data_links[0]["version"]})
    if not sample_ret:
        raise ValueError(f"Retrieved empty sample data for {upa}")

    return sample_ret


def _retrieve_node_data(
        node_tree: list[dict[str, Any]]
) -> dict[str, str | int | float]:
    # retrieve the meta_controlled node data from node tree

    node_data = dict()

    if len(node_tree) != 1:
        raise BadNodeTreeError(f"Expected 1 node in node tree, got {len(node_tree)}")

    sample_node = node_tree[0]

    meta_controlled = sample_node['meta_controlled']
    _check_dict_contains(meta_controlled, [loader_common_names.SAMPLE_LATITUDE, loader_common_names.SAMPLE_LONGITUDE])
    for key, meta_value in meta_controlled.items():
        _validate_node_data(key, meta_value)
        node_data[key] = meta_value['value']

    # create and add geo-spatial data in the format of [latitude, longitude]
    node_data[loader_common_names.SAMPLE_GEO] = [meta_controlled[loader_common_names.SAMPLE_LATITUDE]['value'],
                                                 meta_controlled[loader_common_names.SAMPLE_LONGITUDE]['value']]

    return node_data


def _validate_node_data(key, meta_value):
    # validate meta_value for a given key

    # validate latitude and longitude sample data
    if key in [loader_common_names.SAMPLE_LATITUDE, loader_common_names.SAMPLE_LONGITUDE]:
        expected_keys = ['value', 'units']
        _check_dict_keys(meta_value, expected_keys)

        if meta_value['units'] != 'degrees':
            raise BadNodeTreeError(f"Expected 'units' to be 'degrees', got {meta_value['units']}")
    # validate other general sample data
    else:
        expected_keys = ['value']
        _check_dict_keys(meta_value, expected_keys)


def _check_dict_keys(dictionary: dict[Any, Any], key_list: list[Any]):
    # check if dictionary keys match key list
    if not sorted(dictionary.keys()) == sorted(key_list):
        raise BadNodeTreeError(f"Expected only {key_list} keys in node data, got {dictionary}")


def _check_dict_contains(dictionary: dict[Any, Any], key_list: list[Any]):
    # check if dictionary contains all keys in key list
    if not set(key_list).issubset(dictionary.keys()):
        raise BadNodeTreeError(f"Expected all keys in {key_list} in node data, got {dictionary}")


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
    optional.add_argument(
        "--retrieve_sample",
        action="store_true",
        help="Retrieve sample for each genome object",
    )
    optional.add_argument(
        "--ignore_no_sample_error",
        action="store_true",
        help="Ignore error when no sample data is found for an object",
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
    retrieve_sample = args.retrieve_sample
    ignore_no_sample_error = args.ignore_no_sample_error

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
    csd = None
    if kbase_collection:
        csd = _make_collection_source_dir(
            root_dir,
            loader_common_names.COLLECTION_SOURCE_DIR,
            kbase_collection,
            source_version,
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
            retrieve_sample,
            ignore_no_sample_error
        )

        genome_objs = list_objects(
            workspace_id,
            conf,
            loader_common_names.OBJECTS_NAME_GENOME,
            include_metadata=True,
        )
        assembly_objs = list_objects(
            workspace_id,
            conf,
            loader_common_names.OBJECTS_NAME_ASSEMBLY,
        )
        assembly_genome_map, duplicate_map = _assembly_genome_lookup(genome_objs)
        if duplicate_map:
            for assembly_upa, id_and_date in duplicate_map.items():
                duplicate_map[assembly_upa] = sorted(id_and_date, key=lambda x: x[1])

            duplicate_path = os.path.join(output_dir, GENOME_DUPLICATE_FILE)
            with open(duplicate_path, "w") as outfile:
                json.dump(duplicate_map, outfile)

            print(
                f"Multiple genomes pointing to the same assembly were found, only the latest was kept, "
                f"and the duplicates are in a file at {duplicate_path}"
            )

        upas = []
        for obj_info in assembly_objs:
            upa = "{6}_{0}_{4}".format(*obj_info)
            upas.append(upa)
            genome_upa = assembly_genome_map[upa.replace("_", "/")]
            conf.queue.put([upa, obj_info, genome_upa])

        for i in range(workers + 1):
            conf.queue.put(None)

        conf.pools.close()
        conf.pools.join()

        # create a softlink from the relevant directory under collectionssource
        if csd:
            for upa in upas:
                upa_dir = os.path.join(output_dir, upa)
                csd_upa_dir = os.path.join(csd, upa)
                _create_softlink(csd_upa_dir, upa_dir)
            assert len(os.listdir(csd)) == len(
                assembly_objs
            ), f"directory count in {csd} is not equal to object count"

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
