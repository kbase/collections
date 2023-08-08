"""
usage: workspace_downloader.py [-h] --workspace_id WORKSPACE_ID [--kbase_collection KBASE_COLLECTION] [--source_version SOURCE_VERSION] [--root_dir ROOT_DIR]
                               [--env {CI,NEXT,APPDEV,PROD}] [--workers WORKERS] [--token_filepath TOKEN_FILEPATH] [--keep_job_dir] [--retrieve_sample] [--ignore_no_sample_error]

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
  --root_dir ROOT_DIR   Root directory for the collections project. (default: /global/cfs/cdirs/kbase/collections)
  --env {CI,NEXT,APPDEV,PROD}
                        KBase environment, defaulting to PROD (default: PROD)
  --workers WORKERS     Number of workers for multiprocessing (default: 5)
  --token_filepath TOKEN_FILEPATH
                        A file path that stores KBase token
  --keep_job_dir        Keep SDK job directory after download task is completed
  --retrieve_sample     Retrieve sample for each genome object
  --ignore_no_sample_error
                        Ignore error when no sample data is found for an object

            
e.g.
PYTHONPATH=. python src/loaders/workspace_downloader/workspace_downloader.py --workspace_id 39795 --kbase_collection PMI --source_version 2023.01 --env CI --keep_job_dir

NOTE:
NERSC file structure for WS:
/global/cfs/cdirs/kbase/collections/sourcedata/ -> WS -> ENV -> workspace ID -> UPA -> .fa && .meta files

e.g.
/global/cfs/cdirs/kbase/collections/sourcedata/WS -> CI -> 39795 -> 39795_10_1 -> 39795_10_1.fa
                                                                               -> 39795_10_1.meta
                                                                    39795_22_1 -> 39795_22_1.fa
                                                                               -> 39795_22_1.meta

If kbase_collection and source_version are provided, the data will be linked to the collections source directory:
e.g. /global/cfs/cdirs/kbase/collections/collectionssource/ -> ENV -> kbase_collection -> source_version -> UPA -> .fa && .meta files
"""
import argparse
import json
import os
import shutil
from collections import defaultdict
from multiprocessing import cpu_count
from pathlib import Path
from typing import Any

import src.common.storage.collection_and_field_names as names
from src.loaders.common import loader_common_names, loader_helper
from src.loaders.workspace_downloader.workspace_downloader_helper import Conf

# setup KB_AUTH_TOKEN as env or provide a token_filepath in --token_filepath
# export KB_AUTH_TOKEN="your-kb-auth-token"

# filename that logs genome duplicates for each assembly
GENOME_DUPLICATE_FILE = "duplicate_genomes.json"


class BadNodeTreeError(Exception):
    pass


class NoDataLinkError(Exception):
    pass


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


def _process_input(conf: Conf):
    """
    Download .fa and .meta files from workspace and save a copy under output_dir.
    """
    while True:
        task = conf.input_queue.get(block=True)
        if not task:
            print("Stopping")
            break
        upa, obj_info, genome_upa = task

        upa_dir = os.path.join(conf.output_dir, upa)
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
            _dump_json_to_file(metafile, _process_object_info(obj_info, genome_upa))

            print("Completed %s" % (upa))
        else:
            print(f"Skip downloading {upa} as it already exists")

        if conf.retrieve_sample:
            _download_sample_data(conf, [upa.replace("_", "/"), genome_upa], metafile)


def _download_sample_data(
        conf: Conf,
        upas: list[str],
        metafile: str) -> None:
    # retrieve sample data from sample service and save to file for one and only one upa from input upas
    # additionally, retrieve node data from the sample data and save it to a file

    # check if sample information already exists in the metadata file
    with open(metafile, "r", encoding="utf8") as json_file:
        meta = json.load(json_file)

    sample_keys = [loader_common_names.SAMPLE_FILE_KEY,
                   loader_common_names.SAMPLE_PREPARED_KEY,
                   loader_common_names.SAMPLE_EFFECTIVE_TIME]
    if all(key in meta for key in sample_keys):
        print(f"Skip downloading sample data for {upas} as it already exists")
        return

    # retrieve sample data from sample service and parse key-value node data
    sample_ret, sample_upa, sample_effective_time = _find_sample_upa(conf, upas)
    if not sample_ret:
        if not conf.ignore_no_sample_error:
            raise ValueError(f"Sample data not found for {upas}")
        return

    node_data = _retrieve_node_data(sample_ret['node_tree'])
    node_data[names.FLD_KB_SAMPLE_ID] = sample_ret['id']

    # save sample data and parsed key-value node data to file
    upa_dir, sample_file_prefix = Path(metafile).parent, sample_upa.replace("/", "_")
    sample_file_name = f"{sample_file_prefix}.{loader_common_names.SAMPLE_FILE_EXT}"
    sample_file = os.path.join(upa_dir, sample_file_name)
    sample_prepared_name = f"{sample_file_prefix}.{loader_common_names.SAMPLE_PREPARED_EXT}"
    sample_prepared_file = os.path.join(upa_dir, sample_prepared_name)
    _dump_json_to_file(sample_file, sample_ret)
    _dump_json_to_file(sample_prepared_file, node_data)

    # update metadata file with sample information
    meta[loader_common_names.SAMPLE_FILE_KEY] = sample_file_name
    meta[loader_common_names.SAMPLE_PREPARED_KEY] = sample_prepared_name
    meta[loader_common_names.SAMPLE_EFFECTIVE_TIME] = sample_effective_time
    _dump_json_to_file(metafile, meta)


def _dump_json_to_file(json_file_path: str, json_data: dict[str, Any]) -> None:
    # dump json data to file
    with open(json_file_path, "w", encoding="utf8") as json_file:
        json.dump(json_data, json_file, indent=2)


def _find_sample_upa(
        conf: Conf,
        upas: list[str]
) -> (dict[str, Any], str):
    # find one and only one sample associated upa from input upas and retrieve the sample data
    # raise error if multiple samples are found

    found_sample, sample_ret, sample_upa, sample_effective_time = False, None, None, None

    for upa in upas:
        try:
            sample_ret, sample_effective_time = _retrieve_sample(conf, upa)
            if found_sample:
                raise ValueError(f"Found multiple samples in input {upas}")
            found_sample, sample_upa = True, upa
        except NoDataLinkError:
            pass

    return sample_ret, sample_upa, sample_effective_time


def _retrieve_sample(
        conf: Conf,
        upa: str
) -> (dict[str, Any] | None, int):
    # retrieve sample data from sample service

    # retrieve data links associated with upa
    links_ret = conf.ss.get_data_links_from_data({"upa": upa})

    data_links, effective_time = links_ret['links'], links_ret['effective_time']
    if not data_links:
        raise NoDataLinkError(f"Expected at least 1 data link for {upa}")

    # there should only be one data link for each upa
    if len(data_links) != 1:
        raise ValueError(f"Expected 1 data link for {upa}, got {len(data_links)}")

    # retrieve sample data and save to file
    sample_id = data_links[0]['id']
    sample_ret = conf.ss.get_sample_via_data({"upa": upa,
                                              "id": sample_id,
                                              "version": data_links[0]["version"]})

    return sample_ret, effective_time


def _retrieve_node_data(
        node_tree: list[dict[str, Any]]
) -> dict[str, str | int | float]:
    # retrieve the meta_controlled node data from node tree

    node_data = dict()

    if len(node_tree) != 1:
        raise BadNodeTreeError(f"Expected 1 node in node tree, got {len(node_tree)}")

    sample_node = node_tree[0]

    meta_controlled = sample_node['meta_controlled']
    _check_dict_contains(meta_controlled, [names.FLD_SAMPLE_LATITUDE, names.FLD_SAMPLE_LONGITUDE])
    for key, meta_value in meta_controlled.items():
        _validate_node_data(key, meta_value)
        node_data[key] = meta_value['value']

    # create and add geo-spatial data in the format of [longitude, latitude]
    node_data[names.FLD_SAMPLE_GEO] = [meta_controlled[names.FLD_SAMPLE_LONGITUDE]['value'],
                                       meta_controlled[names.FLD_SAMPLE_LATITUDE]['value']]

    return node_data


def _validate_node_data(key, meta_value):
    # validate meta_value for a given key

    # validate latitude and longitude sample data
    if key in [names.FLD_SAMPLE_LATITUDE, names.FLD_SAMPLE_LONGITUDE]:
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
        f"--{loader_common_names.SOURCE_VER_ARG_NAME}",
        type=str,
        help="Create a source version and link in data to that collection from the overall workspace source data dir",
    )
    optional.add_argument(
        f"--{loader_common_names.ROOT_DIR_ARG_NAME}",
        type=str,
        default=loader_common_names.ROOT_DIR,
        help=loader_common_names.ROOT_DIR_DESCR
    )
    optional.add_argument(
        f"--{loader_common_names.ENV_ARG_NAME}",
        type=str,
        choices=loader_common_names.KB_ENV,
        default='PROD',
        help="KBase environment, defaulting to PROD",
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
    source_version = getattr(args, loader_common_names.SOURCE_VER_ARG_NAME)
    root_dir = getattr(args, loader_common_names.ROOT_DIR_ARG_NAME)
    env = args.env
    workers = args.workers
    token_filepath = args.token_filepath
    keep_job_dir = args.keep_job_dir
    retrieve_sample = args.retrieve_sample
    ignore_no_sample_error = args.ignore_no_sample_error

    kb_base_url = loader_common_names.KB_BASE_URL_MAP[env]

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

    job_dir = loader_helper.make_job_dir(root_dir, username)
    output_dir = loader_helper.make_sourcedata_ws_dir(root_dir, env, workspace_id)
    collection_source_dir = None
    if kbase_collection:
        collection_source_dir = loader_helper.make_collection_source_dir(
            root_dir,
            env,
            kbase_collection,
            source_version,
        )

    proc = None
    conf = None

    try:
        # start podman service
        proc = loader_helper.start_podman_service(uid)

        # set up conf and start callback server
        conf = Conf(
            job_dir,
            output_dir,
            kb_base_url,
            token_filepath,
            loader_common_names.CALLBACK_IMAGE_NAME,
            workers,
            _process_input,
            retrieve_sample,
            ignore_no_sample_error,
        )

        genome_objs = loader_helper.list_objects(
            workspace_id,
            conf,
            loader_common_names.OBJECTS_NAME_GENOME,
            include_metadata=True,
        )
        assembly_objs = loader_helper.list_objects(
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
            conf.input_queue.put([upa, obj_info, genome_upa])

        for i in range(workers + 1):
            conf.input_queue.put(None)

        conf.pools.close()
        conf.pools.join()

        # create a softlink from the relevant directory under collectionssource
        if collection_source_dir:
            loader_helper.create_softlinks_in_collection_source_dir(
                collection_source_dir,
                output_dir,
                upas,
            )
            assert len(os.listdir(collection_source_dir)) == len(
                assembly_objs
            ), f"directory count in {collection_source_dir} is not equal to object count"

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
