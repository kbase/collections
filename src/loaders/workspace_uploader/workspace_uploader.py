"""
usage: workspace_uploader.py [-h] --workspace_id WORKSPACE_ID [--kbase_collection KBASE_COLLECTION]
                             [--source_ver SOURCE_VER] [--root_dir ROOT_DIR] [--load_id LOAD_ID]
                             [--token_filepath TOKEN_FILEPATH] [--env {CI,NEXT,APPDEV,PROD}]
                             [--upload_file_ext UPLOAD_FILE_EXT [UPLOAD_FILE_EXT ...]] [--batch_size BATCH_SIZE]
                             [--cbs_max_tasks CBS_MAX_TASKS] [--au_service_ver AU_SERVICE_VER]
                             [--gfu_service_ver GFU_SERVICE_VER] [--keep_job_dir] [--as_catalog_admin]

PROTOTYPE - Upload files to the workspace service (WSS). Note that the uploader determines whether a genome is already uploaded in
one of two ways. First it consults the *.yaml files in each genomes directory; if that file shows the genome has been uploaded it skips it
regardless of the current state of the workspace. Second, it checks that the most recent version of the genome object in the workspace, if it
exists, was part of the current load ID (see the load ID parameter description below). If so, the genome is skipped.

options:
  -h, --help            show this help message and exit

required named arguments:
  --workspace_id WORKSPACE_ID
                        Target workspace addressed by the permanent ID
  --kbase_collection KBASE_COLLECTION
                        The name of the collection being processed
                        Specifies where the files to be uploaded exist (in the default NONE environment)
                        and the name of the collection to be created in the specific KBase environment in the 'collectionsource' directory
  --source_ver SOURCE_VER
                        The source version of the collection being processed
                        Specifies where the files to be uploaded exist (in the default NONE environment)
                        and the source version of the collection to be created in the specific KBase environment in the 'collectionsource' directory

optional arguments:
  --root_dir ROOT_DIR   Root directory for the collections project. (default: /global/cfs/cdirs/kbase/collections)
  --load_id LOAD_ID     The load id of the objects being uploaded to a workspace. If not provided, a random load_id will be generated. For a
                        particular load, any restarts / resumes of the load should use the same load ID to prevent reuploading the same data.
                        A new load ID will make new versions of all the objects from the prior upload.
  --token_filepath TOKEN_FILEPATH
                        A file path that stores a KBase token appropriate for the KBase environment.
                        If not provided, the token must be provided in the `KB_AUTH_TOKEN` environment variable.
  --env {CI,NEXT,APPDEV,PROD}
                        KBase environment (default: PROD)
  --upload_file_ext UPLOAD_FILE_EXT [UPLOAD_FILE_EXT ...]
                        Upload only files that match given extensions. (default: ['genomic.gbff.gz'])
  --batch_size BATCH_SIZE
                        Number of files to upload per batch (default: 2500)
  --cbs_max_tasks CBS_MAX_TASKS
                        The maximum number of subtasks for the callback server (default: 20)
  --au_service_ver AU_SERVICE_VER
                        The service version of AssemblyUtil client('dev', 'beta', 'release', or a git commit) (default: release)
  --gfu_service_ver GFU_SERVICE_VER
                        The service version of GenomeFileUtil client('dev', 'beta', 'release', or a git commit) (default: release)
  --keep_job_dir        Keep SDK job directory after upload task is completed
  --as_catalog_admin    True means the provided user token has catalog admin privileges and will be used to retrieve secure SDK app
                        parameters from the catalog. If false, the default, SDK apps run as part of this application will not have access to
                        catalog secure parameters.

e.g.
PYTHONPATH=. python src/loaders/workspace_uploader/workspace_uploader.py --workspace_id 69046 --kbase_collection GTDB --source_ver 207 --env CI --keep_job_dir

For more information regarding file structure, please refer to the 'Workspace Uploader' section in the documentation
at docs/data_pipeline_procedure.md.
"""

import argparse
import os
import shutil
import subprocess
import time
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Generator

import click
import yaml

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.GenomeFileUtilClient import GenomeFileUtil
from src.clients.workspaceClient import Workspace
from src.common.common_helper import obj_info_to_upa
from src.loaders.common import loader_common_names, loader_helper
from src.loaders.common.callback_server_wrapper import Conf
from src.loaders.workspace_uploader.upload_result import UploadResult, WSObjTuple

# setup KB_AUTH_TOKEN as env or provide a token_filepath in --token_filepath
# export KB_AUTH_TOKEN="your-kb-auth-token"

_UPLOAD_GENOME_FILE_EXT = ["genomic.gbff.gz"]
_JOB_DIR_IN_CONTAINER = "/kb/module/work/tmp"
_UPLOADED_YAML = "uploaded.yaml"
_WS_MAX_BATCH_SIZE = 10000

# keys for the uploaded.yaml file
_KEY_ASSEMBLY_UPA = "assembly_upa"
_KEY_ASSEMBLY_FILENAME = "assembly_filename"
_KEY_GENOME_UPA = "genome_upa"
_KEY_GENOME_FILENAME = "genome_filename"


def _get_parser():
    parser = argparse.ArgumentParser(
        description="PROTOTYPE - Upload files to the workspace service (WSS).\n\n"
        "Note that the uploader determines whether a genome is already uploaded in one of two ways. "
        "First it consults the *.yaml files in each genomes directory; if that file shows the genome "
        "has been uploaded it skips it regardless of the current state of the workspace. "
        "Second, it checks that the most recent version of the genome object in the workspace, "
        "if it exists, was part of the current load ID (see the load ID parameter description below). "
        "If so, the genome is skipped.",
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
        help="The name of the collection being processed. "
        "Specifies where the files to be uploaded exist (in the default NONE environment) "
        "and the name of the collection to be created in the specific KBase environment in the 'collectionsource' directory.",
    )
    required.add_argument(
        f"--{loader_common_names.SOURCE_VER_ARG_NAME}",
        type=str,
        help="The source version of the collection being processed. "
        "Specifies where the files to be uploaded exist (in the default NONE environment) "
        "and the source version of the collection to be created in the specific KBase environment in the 'collectionsource' directory.",
    )

    # Optional argument
    optional.add_argument(
        f"--{loader_common_names.ROOT_DIR_ARG_NAME}",
        type=str,
        default=loader_common_names.ROOT_DIR,
        help=loader_common_names.ROOT_DIR_DESCR,
    )
    optional.add_argument(
        "--load_id",
        type=str,
        help="The load id of the objects being uploaded to a workspace. "
        "If not provided, a random load_id will be generated. "
        "For a particular load, any restarts / resumes of the load should use the same load ID to prevent reuploading the same data. "
        "A new load ID will make new versions of all the objects from the prior upload.",
    )
    optional.add_argument(
        "--token_filepath",
        type=str,
        help="A file path that stores a KBase token appropriate for the KBase environment. "
        "If not provided, the token must be provided in the `KB_AUTH_TOKEN` environment variable.",
    )
    optional.add_argument(
        "--env",
        type=str,
        choices=loader_common_names.KB_ENV,
        default="PROD",
        help="KBase environment",
    )
    optional.add_argument(
        "--upload_file_ext",
        type=str,
        default=_UPLOAD_GENOME_FILE_EXT,
        nargs="+",
        help="Upload only files that match given extensions.",
    )
    optional.add_argument(
        "--batch_size",
        type=int,
        default=2500,
        help="Number of files to upload per batch",
    )
    optional.add_argument(
        "--cbs_max_tasks",
        type=int,
        default=20,
        help="The maximum number of subtasks for the callback server",
    )
    optional.add_argument(
        "--au_service_ver",
        type=str,
        default="release",
        help="The service version of AssemblyUtil client"
        "('dev', 'beta', 'release', or a git commit)",
    )
    optional.add_argument(
        "--gfu_service_ver",
        type=str,
        default="release",
        help="The service version of GenomeFileUtil client"
        "('dev', 'beta', 'release', or a git commit)",
    )
    optional.add_argument(
        "--keep_job_dir",
        action="store_true",
        help="Keep SDK job directory after upload task is completed",
    )
    optional.add_argument(
        "--as_catalog_admin",
        action="store_true",
        help="True means the provided user token has catalog admin privileges and will "
        "be used to retrieve secure SDK app parameters from the catalog. If false, the default, "
        "SDK apps run as part of this application will not have access to catalog secure parameters.",
    )
    return parser


def _get_yaml_file_path(obj_dir: str) -> str:
    """
    Get the uploaded.yaml file path from collections source directory.
    """
    file_path = os.path.join(obj_dir, _UPLOADED_YAML)
    Path(file_path).touch(exist_ok=True)
    return file_path


def _get_source_file(obj_dir: str, obj_file: str) -> str:
    """
    Get the sourcedata file path from the WS object directory.
    """
    if not os.path.islink(obj_dir):
        raise ValueError(f"{obj_dir} is not a symlink")
    src_file = os.path.join(os.readlink(obj_dir), obj_file)
    return src_file


def _upload_genomes_to_workspace(
    gfu: GenomeFileUtil,
    workspace_id: int,
    load_id: str,
    ws_obj_tuples: list[WSObjTuple],
    job_data_dir: str,
) -> list[UploadResult]:
    """
    Upload genbank files to the target workspace as Genome in batch. The bulk method fails
    and an error will be thrown if any of the genome files in batch fails to upload.
    The order of elements in the returned list corresponds to the order of `ws_obj_tuples`.
    """

    inputs = [
        {
            "file": {'path': obj_tuple.container_internal_file},
            "genome_name": obj_tuple.obj_name,
            "metadata": {"load_id": load_id},
        }
        for obj_tuple in ws_obj_tuples
    ]

    results = gfu.genbanks_to_genomes(
        {"workspace_id": workspace_id, "inputs": inputs}
    )["results"]

    upload_results = []
    for result_dict, obj_tuple in zip(results, ws_obj_tuples):

        genome_obj_info = result_dict["genome_info"]
        assembly_obj_info = result_dict["assembly_info"]

        genome_tuple = obj_tuple

        # copy assembly file from tmp job dir to the sourcedata/NCBI directory
        container_assembly_path = Path(result_dict["assembly_path"])
        # remove prefix of the assembly_path
        relative_assembly_path = container_assembly_path.relative_to(_JOB_DIR_IN_CONTAINER)
        local_assembly_path = Path(job_data_dir) / relative_assembly_path
        if not os.path.exists(local_assembly_path):
            raise ValueError(f"Assembly file {local_assembly_path} does not exist")

        collection_source_data_dir = Path(genome_tuple.obj_coll_src_dir)
        # TODO: this file is overwritten when a different GTDB version is uploaded.
        #  If this poses a concern, we should revisit and update file_name here and logic for writing to the upload.yaml file.
        fasta_file_name = container_assembly_path.name
        # links the assembly file created by GFU into the collectionsource directory that is the upload data source
        # for example:
        #  root/collectionssource/NONE/GTDB/v214/<genome_id>/<assembly file name>
        loader_helper.create_hardlink_between_files(collection_source_data_dir / fasta_file_name,
                                                    local_assembly_path)

        assembly_tuple = WSObjTuple(
            obj_name=assembly_obj_info[1],
            obj_file_name=fasta_file_name,
            obj_coll_src_dir=collection_source_data_dir,
            container_internal_file=container_assembly_path,
        )

        upload_result = UploadResult(
            genome_obj_info=genome_obj_info,
            assembly_obj_info=assembly_obj_info,
            genome_tuple=genome_tuple,
            assembly_tuple=assembly_tuple
        )

        upload_results.append(upload_result)

    return upload_results


def _read_upload_status_yaml_file(
    upload_env_key: str,
    workspace_id: int,
    load_id: str,
    obj_dir: str,
) -> tuple[dict[str, dict[int, list[str]]], bool]:
    """
    Get metadata and upload status of a WS object from the uploaded.yaml file.

    Structure of result yaml file:
    <env>:
        <workspace_id>:
            <load_id>:
                assembly_upa: <assembly_upa>
                assembly_filename: <assembly_filename>
                genome_upa: <genome_upa>
                genome_filename: <genome_filename>
    """

    if upload_env_key not in loader_common_names.KB_ENV:
        raise ValueError(
            f"Currently only support these {loader_common_names.KB_ENV} envs for upload"
        )

    file_path = _get_yaml_file_path(obj_dir)

    with open(file_path, "r") as file:
        data = yaml.safe_load(file) or dict()

    workspace_dict = data.setdefault(upload_env_key, {}).setdefault(workspace_id, {})

    uploaded = load_id in workspace_dict and workspace_dict[load_id].get(_KEY_GENOME_UPA)

    return data, uploaded


def _update_upload_status_yaml_file(
    upload_env_key: str,
    workspace_id: int,
    load_id: str,
    upload_result: UploadResult,
) -> None:
    """
    Update the uploaded.yaml file in target genome_dir with newly uploaded WS object names and upa info.
    """
    obj_tuple = upload_result.genome_tuple

    data, uploaded = _read_upload_status_yaml_file(
        upload_env_key,
        workspace_id,
        load_id,
        obj_tuple.obj_coll_src_dir,
    )

    if uploaded:
        raise ValueError(
            f"Object {obj_tuple.obj_name} already exists in workspace {workspace_id}"
        )

    data[upload_env_key][workspace_id][load_id] = {
        _KEY_ASSEMBLY_UPA: upload_result.assembly_upa,
        _KEY_ASSEMBLY_FILENAME: upload_result.assembly_tuple.obj_file_name,
        _KEY_GENOME_UPA: upload_result.genome_upa,
        _KEY_GENOME_FILENAME: upload_result.genome_tuple.obj_file_name,
    }

    file_path = _get_yaml_file_path(obj_tuple.obj_coll_src_dir)
    with open(file_path, "w") as file:
        yaml.dump(data, file)


def _fetch_objects_to_upload(
    upload_env_key: str,
    workspace_id: int,
    load_id: str,
    external_coll_src_dir: str,
    upload_file_ext: list[str],
) -> tuple[int, dict[str, str]]:
    count = 0
    wait_to_upload_objs = dict()

    obj_dirs = [
        os.path.join(external_coll_src_dir, d)
        for d in os.listdir(external_coll_src_dir)
        if os.path.isdir(os.path.join(external_coll_src_dir, d))
    ]

    for obj_dir in obj_dirs:
        obj_file_list = [
            f
            for f in os.listdir(obj_dir)
            if os.path.isfile(os.path.join(obj_dir, f))
            and f.endswith(tuple(upload_file_ext))
        ]

        # Genome (from genbank) object uploader only requires one file
        # Modify or skip this check if a different use case requires multiple files.
        if len(obj_file_list) != 1:
            raise ValueError(
                f"One and only one object file that ends with {upload_file_ext} "
                f"must be present in {obj_dir} directory"
            )

        count += 1
        obj_name = obj_file_list[0]

        _, uploaded = _read_upload_status_yaml_file(
            upload_env_key,
            workspace_id,
            load_id,
            obj_dir,
        )

        if uploaded:
            print(
                f"Object {obj_name} already exists in "
                f"workspace {workspace_id} load {load_id}. Skipping."
            )
            continue

        wait_to_upload_objs[obj_name] = obj_dir

    return count, wait_to_upload_objs


def _query_workspace_with_load_id(
    ws: Workspace,
    workspace_id: int,
    load_id: str,
    obj_names: list[str],
) -> tuple[list[Any], list[Any]]:
    if len(obj_names) > _WS_MAX_BATCH_SIZE:
        raise ValueError(
            f"The effective max batch size must be <= {_WS_MAX_BATCH_SIZE}"
        )
    refs = [{"wsid": workspace_id, "name": name} for name in obj_names]
    res = ws.get_object_info3({"objects": refs, "ignoreErrors": 1, "includeMetadata": 1})
    uploaded_objs_info = [info for info in res["infos"] if info and info[10].get("load_id") == load_id]
    if not uploaded_objs_info:
        return list(), list()

    _check_obj_type(workspace_id, load_id, uploaded_objs_info, {loader_common_names.OBJECTS_NAME_GENOME})
    genome_objs_info = uploaded_objs_info
    assembly_objs_ref = list()
    for info in genome_objs_info:
        try:
            assembly_objs_ref.append(info[10]["Assembly Object"])
        except KeyError:
            genome_ref = obj_info_to_upa(info)
            raise ValueError(
                f"Genome object {genome_ref} does not have an assembly object linked to it"
            )

    assembly_objs_spec = [{"ref": ref} for ref in assembly_objs_ref]
    assembly_objs_info = ws.get_object_info3({"objects": assembly_objs_spec, "includeMetadata": 1})["infos"]

    return assembly_objs_info, genome_objs_info


def _check_obj_type(
    workspace_id: int,
    load_id: str,
    obj_infos: list[Any],
    expected_obj_types: set[str],
):
    obj_types = {info[2].split("-")[0] for info in obj_infos}
    if obj_types != expected_obj_types:
        raise ValueError(
            f"Only expecting {sorted(expected_obj_types)} objects. "
            f"However, found {sorted(obj_types)} in the workspace {workspace_id} with load {load_id}")


def _query_workspace_with_load_id_mass(
    ws: Workspace,
    workspace_id: int,
    load_id: str,
    obj_names: list[str],
    batch_size: int = _WS_MAX_BATCH_SIZE,
) -> tuple[list[Any], list[Any]]:

    uploaded_assembly_objs_info, uploaded_genome_objs_info = [], []

    for i in range(0, len(obj_names), batch_size):
        (uploaded_assembly_objs_info_batch,
         uploaded_genome_objs_info_batch) = _query_workspace_with_load_id(ws,
                                                                          workspace_id,
                                                                          load_id,
                                                                          obj_names[i:i + batch_size])

        uploaded_assembly_objs_info.extend(uploaded_assembly_objs_info_batch)
        uploaded_genome_objs_info.extend(uploaded_genome_objs_info_batch)

    return uploaded_assembly_objs_info, uploaded_genome_objs_info


def _prepare_skd_job_dir_to_upload(
    conf: Conf, wait_to_upload_objs: dict[str, str]
) -> str:
    """
    Prepare SDK job directory to upload.
    """
    for obj_file, obj_dir in wait_to_upload_objs.items():
        src_file = _get_source_file(obj_dir, obj_file)
        dest_file = os.path.join(conf.job_data_dir, obj_file)
        loader_helper.create_hardlink_between_files(dest_file, src_file)

    return conf.job_data_dir


def _post_process(
    upload_env_key: str,
    workspace_id: int,
    load_id: str,
    ws_coll_src_dir: str,
    source_data_dir: str,
    upload_result: UploadResult,
) -> None:
    """
    Update the uploaded.yaml file in the genome directory with the object name and upa info.

    The function will also:
    Create a standard entry in sourcedata/workspace for each object.
    Hardlink to the original object file in sourcedata to avoid duplicating the file.
    Creates a softlink from new_dir in collectionssource to the contents of target_dir in sourcedata.
    """

    _process_genome_objects(ws_coll_src_dir,
                            source_data_dir,
                            upload_result,
                            )

    # Update the 'uploaded.yaml' file, serving as a marker to indicate the successful upload of the object.
    # Ensure that this operation is the final step in the post-processing workflow
    _update_upload_status_yaml_file(
        upload_env_key,
        workspace_id,
        load_id,
        upload_result,
    )


def _process_genome_objects(
        ws_coll_src_dir: str,
        source_data_dir: str,
        upload_result: UploadResult,
) -> None:
    """
    Post process on successful genome uploads.
    """
    # create hardlink for the FASTA file from upload data collection source directory (e.g. GTDB)
    # to the corresponding workspace object directory.
    assembly_tuple, assembly_upa = upload_result.assembly_tuple, upload_result.assembly_upa
    coll_src_assembly = Path(_get_source_file(assembly_tuple.obj_coll_src_dir, assembly_tuple.obj_file_name))
    ws_source_data_dir = os.path.join(source_data_dir, assembly_upa)
    os.makedirs(ws_source_data_dir, exist_ok=True)

    suffixes = coll_src_assembly.suffixes
    # TODO - handle extension other than .gz
    dest_suffix = suffixes[-1] if suffixes[-1] != ".gz" else "".join(suffixes[-2:])
    ws_src_assembly = os.path.join(ws_source_data_dir, f"{assembly_upa}{dest_suffix}")
    loader_helper.create_hardlink_between_files(ws_src_assembly, coll_src_assembly)

    # create metadata file used by parser
    loader_helper.create_meta_file(source_data_dir,
                                   assembly_upa,
                                   upload_result.assembly_obj_info,
                                   upload_result.genome_obj_info)

    # create a softlink from new_dir in collectionssource to the contents of target_dir in sourcedata
    new_dir = os.path.join(ws_coll_src_dir, assembly_upa)
    loader_helper.create_softlink_between_dirs(new_dir, ws_source_data_dir)


def _download_assembly_fasta_file(
        asu_client: AssemblyUtil,
        genome_obj_info: list[Any],
        coll_src_assembly_path: str,
        job_data_dir: Path | str,
) -> Path:
    """
    Download assembly fasta file from the workspace using AssemblyUtil.
    Copy the assembly file from the job directory to the collection source directory.
    """
    try:
        assembly_upa = genome_obj_info[10]["Assembly Object"]
    except KeyError:
        raise ValueError(f"Genome object {genome_obj_info[1]} does not have an assembly object linked to it")
    containerized_assembly_file = Path(asu_client.get_assembly_as_fasta({"ref": assembly_upa})["path"])
    # remove prefix of the assembly_path
    relative_assembly_path = containerized_assembly_file.relative_to(_JOB_DIR_IN_CONTAINER)
    local_assembly_file = Path(job_data_dir) / relative_assembly_path

    loader_helper.create_hardlink_between_files(coll_src_assembly_path,
                                                local_assembly_file)

    return containerized_assembly_file


def _build_assembly_tuples(
    genome_tuples: list[WSObjTuple],
    genome_obj_infos: list[Any],
    asu_client: AssemblyUtil,
    job_data_dir: str,
) -> list[WSObjTuple]:
    """
    Build assembly tuples from genome tuples. Retrieve assembly fasta file from the workspace if it does not exist.
    """
    assembly_tuples = list()

    for genome_tuple, genome_obj_info in zip(genome_tuples, genome_obj_infos):
        assembly_file = genome_tuple.obj_name + "_assembly.fasta"  # matches the FASTA file name returned from GFU during successful upload process
        coll_src_assembly_path = os.path.join(genome_tuple.obj_coll_src_dir, assembly_file)
        if not os.path.exists(coll_src_assembly_path):
            _download_assembly_fasta_file(asu_client,
                                          genome_obj_info,
                                          coll_src_assembly_path,
                                          job_data_dir)

        assembly_tuple = WSObjTuple(
            obj_name=genome_tuple.obj_name + "_assembly",  # aligns with the Assembly object name generated by GFU. Given that the assembly object name isn't utilized during failure recovery, there's no need to invoke a workspace call to retrieve it.
            obj_file_name=assembly_file,
            obj_coll_src_dir=genome_tuple.obj_coll_src_dir,
            container_internal_file=None,  # container_internal_file is fine to be None here as it's not used during failure recovery
        )
        assembly_tuples.append(assembly_tuple)

    return assembly_tuples


def _process_failed_uploads(
        ws: Workspace,
        workspace_id: int,
        load_id: str,
        obj_tuples: list[WSObjTuple],
        asu_client: AssemblyUtil,
        job_data_dir: str,
) -> list[UploadResult]:

    # figure out uploads that succeeded
    upload_results = list()
    name2tuple = {obj_tuple.obj_name: obj_tuple for obj_tuple in obj_tuples}

    (uploaded_assembly_obj_infos,
     uploaded_genome_obj_infos) = _query_workspace_with_load_id_mass(ws,
                                                                     workspace_id,
                                                                     load_id,
                                                                     list(name2tuple.keys())
                                                                     )

    batch_uploaded_genome_tuples = [name2tuple[info[1]] for info in uploaded_genome_obj_infos]
    batch_assembly_tuples = _build_assembly_tuples(batch_uploaded_genome_tuples,
                                                   uploaded_genome_obj_infos,
                                                   asu_client,
                                                   job_data_dir)
    for (genome_obj_info,
         assembly_obj_info,
         genome_tuple,
         assembly_tuple) in zip(uploaded_genome_obj_infos,
                                uploaded_assembly_obj_infos,
                                batch_uploaded_genome_tuples,
                                batch_assembly_tuples):
        upload_result = UploadResult(
            genome_obj_info=genome_obj_info,
            assembly_obj_info=assembly_obj_info,
            genome_tuple=genome_tuple,
            assembly_tuple=assembly_tuple,
        )
        upload_results.append(upload_result)

    return upload_results


def _upload_objects_in_parallel(
        ws: Workspace,
        upload_env_key: str,
        workspace_id: int,
        load_id: str,
        ws_coll_src_dir: str,
        wait_to_upload_objs: dict[str, str],
        batch_size: int,
        source_data_dir: str,
        asu_client: AssemblyUtil,
        gfu_client: GenomeFileUtil,
        job_data_dir: str,
) -> int:
    """
    Upload objects to the target workspace in parallel using multiprocessing.

    Parameters:
        ws: Workspace client
        upload_env_key: environment variable key in uploaded.yaml file
        workspace_id: target workspace id
        load_id: load id
        ws_coll_src_dir: a directory in collectionssource representing workspace that creates new directories linking to sourcedata.  i.e. /root_dir/collectionssource/<WS_ENV>/<KBASE_COLLECTION>/<SOURCE_VER>
        wait_to_upload_objs: a dictionary that maps object file name to object directory
        batch_size: a number of files to upload per batch
        source_data_dir: directory for all source data. i.e. /root_dir/sourcedata
        asu_client: AssemblyUtil client
        gfu_client: GenomeFileUtil client
        job_data_dir: the job directory to store object files

    Returns:
        number of object files have been successfully uploaded from wait_to_upload_objs
    """
    objects_len = len(wait_to_upload_objs)
    print(f"Start uploading {objects_len} objects\n")

    uploaded_count = 0
    uploaded_fail = False
    upload_results = list()
    for obj_tuples in _gen(wait_to_upload_objs, batch_size):
        try:
            upload_results = _upload_genomes_to_workspace(gfu_client,
                                                          workspace_id,
                                                          load_id,
                                                          obj_tuples,
                                                          job_data_dir)

        except Exception as e:
            traceback.print_exc()
            uploaded_fail = True
            try:
                upload_results = _process_failed_uploads(ws,
                                                         workspace_id,
                                                         load_id,
                                                         obj_tuples,
                                                         asu_client,
                                                         job_data_dir)
            except Exception as e:
                print(
                    f"WARNING: There are inconsistencies between "
                    f"the workspace and the yaml files as the result of {e}\n"
                    f"Run the script again to attempt resolution."
                )

        # post process on successful uploads
        for upload_result in upload_results:
            _post_process(
                upload_env_key,
                workspace_id,
                load_id,
                ws_coll_src_dir,
                source_data_dir,
                upload_result
            )

        uploaded_count += len(upload_results)
        if uploaded_count % 100 == 0:
            print(
                f"Objects uploaded: {uploaded_count}/{objects_len}, "
                f"Percentage: {uploaded_count / objects_len * 100:.2f}%, "
                f"Time: {datetime.now()}"
            )

        if uploaded_fail:
            return uploaded_count

    return uploaded_count


def _dict2tuple_list(objs_dict: dict[str, str]) -> list[WSObjTuple]:
    ws_object_tuple_list = [
        WSObjTuple(
            obj_name=i[0],
            obj_file_name=i[0],
            obj_coll_src_dir=i[1],
            container_internal_file=os.path.join(_JOB_DIR_IN_CONTAINER, i[0]),
        )
        for i in objs_dict.items()
    ]
    return ws_object_tuple_list


def _gen(
    wait_to_upload_objs: dict[str, str],
    batch_size: int,
) -> Generator[list[WSObjTuple], None, None]:
    """
    Generator function to yield the object files to upload.
    """
    obj_tuple_list = _dict2tuple_list(wait_to_upload_objs)
    # yield WSObjTuples in batch
    for idx in range(0, len(wait_to_upload_objs), batch_size):
        yield obj_tuple_list[idx: idx + batch_size]


def _check_existing_uploads_and_recovery(
        ws: Workspace,
        env: str,
        workspace_id: int,
        load_id: str,
        ws_coll_src_dir: str,
        source_dir: str,
        wait_to_upload_objs: dict[str, str],
        asu_client: AssemblyUtil,
        job_data_dir: str,
) -> list[str]:
    """
    Process existing uploads, perform post-processing, and handle recovery.
    """
    obj_names_processed = []

    wait_to_upload_tuples = _dict2tuple_list(wait_to_upload_objs)
    upload_results = _process_failed_uploads(
        ws,
        workspace_id,
        load_id,
        wait_to_upload_tuples,
        asu_client,
        job_data_dir,
    )

    # Fix inconsistencies between the workspace and the local yaml files
    if upload_results:
        print("Start failure recovery process ...")
        for upload_result in upload_results:
            _post_process(
                env,
                workspace_id,
                load_id,
                ws_coll_src_dir,
                source_dir,
                upload_result
            )
            obj_names_processed.append(upload_result.genome_tuple.obj_name)

        print("Recovery process completed ...")

    return obj_names_processed


def _cleanup_resources(
        conf: Conf,
        proc: subprocess.Popen,
        job_dir: str,
        keep_job_dir: bool):
    """
    Cleanup resources including stopping callback server, terminating Podman service, and removing job directory.
    """

    # Stop callback server if it is on
    if conf:
        conf.stop_callback_server()

    # Stop Podman service if it is on
    if proc:
        proc.terminate()

    # Remove job directory if needed
    if not keep_job_dir:
        shutil.rmtree(job_dir)


def _setup_and_start_services(
        job_dir,
        kb_base_url,
        token_filepath,
        cbs_max_tasks,
        catalog_admin
) -> tuple[subprocess.Popen | None, Conf | None]:
    """
    Setup and start services including Podman service and callback server.
    """

    # Setup container.conf file for the callback server logs if needed
    if loader_helper.is_config_modification_required():
        if click.confirm(
                f"The config file at {loader_common_names.CONTAINERS_CONF_PATH}\n"
                f"needs to be modified to allow for container logging.\n"
                f"Params 'seccomp_profile' and 'log_driver' will be added/updated under section [containers]. Do so now?\n"
        ):
            loader_helper.setup_callback_server_logs()
        else:
            print("Permission denied and exiting ...")
            return None, None

    # Start podman service
    uid = os.getuid()
    proc = loader_helper.start_podman_service(uid)

    # Set up configuration for uploader
    conf = Conf(
        job_dir=job_dir,
        kb_base_url=kb_base_url,
        token_filepath=token_filepath,
        max_callback_server_tasks=cbs_max_tasks,
        catalog_admin=catalog_admin,
    )

    return proc, conf


def _get_parser_args():
    parser = _get_parser()
    args = parser.parse_args()

    workspace_id = args.workspace_id
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    source_version = getattr(args, loader_common_names.SOURCE_VER_ARG_NAME)
    root_dir = getattr(args, loader_common_names.ROOT_DIR_ARG_NAME)
    token_filepath = args.token_filepath
    upload_file_ext = args.upload_file_ext
    batch_size = args.batch_size
    cbs_max_tasks = args.cbs_max_tasks
    au_service_ver = args.au_service_ver
    gfu_service_ver = args.gfu_service_ver
    keep_job_dir = args.keep_job_dir
    catalog_admin = args.as_catalog_admin
    load_id = args.load_id or uuid.uuid4().hex
    print(
        f"load_id is {load_id}.\n"
        f"Please keep using this load version until the load is complete!"
    )
    env = args.env
    kb_base_url = loader_common_names.KB_BASE_URL_MAP[env]

    if workspace_id <= 0:
        parser.error(f"workspace_id needs to be > 0")
    if batch_size <= 0:
        parser.error(f"batch_size needs to be > 0")
    if cbs_max_tasks <= 0:
        parser.error(f"cbs_max_tasks needs to be > 0")

    return (workspace_id, kbase_collection, source_version, root_dir, token_filepath,
            upload_file_ext, batch_size, cbs_max_tasks, au_service_ver, gfu_service_ver,
            keep_job_dir, catalog_admin, load_id, env, kb_base_url)


def _prepare_directories(
        root_dir: str,
        username: str,
        kbase_collection: str,
        source_version: str,
        env: str,
        workspace_id: int,
) -> tuple[str, str, str, str]:
    """
    Prepare directories used for the uploader.
    """
    job_dir = loader_helper.make_job_dir(root_dir, username)
    external_coll_src_dir = loader_helper.make_collection_source_dir(
        root_dir, loader_common_names.DEFAULT_ENV, kbase_collection, source_version
    )
    ws_coll_src_dir = loader_helper.make_collection_source_dir(
        root_dir, env, kbase_collection, source_version
    )
    source_dir = loader_helper.make_sourcedata_ws_dir(root_dir, env, workspace_id)

    return job_dir, external_coll_src_dir, ws_coll_src_dir, source_dir


def _create_kbase_clients(
        kb_base_url,
        callback_url,
        token,
        au_service_ver,
        gfu_service_ver
) -> tuple[Workspace, AssemblyUtil, GenomeFileUtil]:
    """
    Create workspace clients.
    """
    ws_url = os.path.join(kb_base_url, "ws")
    ws = Workspace(ws_url, token=token)
    asu_client = AssemblyUtil(callback_url, service_ver=au_service_ver, token=token)
    gfu_client = GenomeFileUtil(callback_url, service_ver=gfu_service_ver, token=token)

    return ws, asu_client, gfu_client


def _fetch_objs_to_upload(
        env: str,
        ws: Workspace,
        workspace_id: int,
        load_id: str,
        external_coll_src_dir: str,
        ws_coll_src_dir: str,
        source_dir: str,
        upload_file_ext: list[str],
        asu_client: AssemblyUtil,
        job_data_dir: str
) -> dict[str, str]:
    """
    Fetch objects to be uploaded to the workspace.

    Check if the objects are already uploaded to the workspace and perform recovery if needed.
    """
    count, wait_to_upload_objs = _fetch_objects_to_upload(
        env, workspace_id, load_id, external_coll_src_dir, upload_file_ext)

    # check if the objects are already uploaded to the workspace
    obj_names_processed = _check_existing_uploads_and_recovery(
        ws,
        env,
        workspace_id,
        load_id,
        ws_coll_src_dir,
        source_dir,
        wait_to_upload_objs,
        asu_client,
        job_data_dir,
    )

    for obj_name in obj_names_processed:
        wait_to_upload_objs.pop(obj_name)

    if not wait_to_upload_objs:
        print(f"All {count} files already exist in workspace {workspace_id}")
        return wait_to_upload_objs

    wtus_len = len(wait_to_upload_objs)
    print(f"Originally planned to upload {count} object files")
    print(f"Detected {count - wtus_len} object files already exist in workspace")

    return wait_to_upload_objs


def main():

    (workspace_id, kbase_collection, source_version, root_dir, token_filepath,
     upload_file_ext, batch_size, cbs_max_tasks, au_service_ver, gfu_service_ver,
     keep_job_dir, catalog_admin, load_id, env, kb_base_url) = _get_parser_args()

    username = os.getlogin()
    job_dir, external_coll_src_dir, ws_coll_src_dir, source_dir = _prepare_directories(
        root_dir, username, kbase_collection, source_version, env, workspace_id
    )

    proc = None
    conf = None
    try:
        proc, conf = _setup_and_start_services(job_dir, kb_base_url, token_filepath, cbs_max_tasks, catalog_admin)
        if not proc or not conf:
            print("Failed to start services. Exiting ...")
            return

        ws, asu_client, gfu_client = _create_kbase_clients(
            kb_base_url, conf.callback_url, conf.token, au_service_ver, gfu_service_ver)

        wait_to_upload_objs = _fetch_objs_to_upload(
            env, ws, workspace_id, load_id, external_coll_src_dir, ws_coll_src_dir, source_dir, upload_file_ext, asu_client, conf.job_data_dir
        )
        if not wait_to_upload_objs:
            return

        _prepare_skd_job_dir_to_upload(conf, wait_to_upload_objs)
        print(f"{len(wait_to_upload_objs)} objects in {conf.job_data_dir} are ready to upload to workspace {workspace_id}")

        start = time.time()

        uploaded_count = _upload_objects_in_parallel(
            ws,
            env,
            workspace_id,
            load_id,
            ws_coll_src_dir,
            wait_to_upload_objs,
            batch_size,
            source_dir,
            asu_client,
            gfu_client,
            conf.job_data_dir,
        )

        upload_time = (time.time() - start) / 60
        assy_per_min = uploaded_count / upload_time

        print(
            f"\ntook {upload_time:.2f} minutes to upload {uploaded_count} objects, "
            f"averaging {assy_per_min:.2f} objects per minute"
        )

    finally:
        _cleanup_resources(conf, proc, job_dir, keep_job_dir)


if __name__ == "__main__":
    main()
