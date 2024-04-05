import argparse
import math
import os
import shutil

import src.loaders.jobs.taskfarmer.taskfarmer_common as tf_common
from src.loaders.common import loader_common_names
from src.loaders.common.loader_helper import make_collection_source_dir
from src.loaders.compute_tools.tool_version import extract_latest_version, extract_latest_reference_db_version
from src.loaders.jobs.taskfarmer.taskfarmer_task_mgr import TFTaskManager, PreconditionError

'''

Create the required documents and scripts for the TaskFarmer Workflow Manager and provide the option to execute tasks.

usage: task_generator.py [-h] --tool {gtdb_tk,checkm2,microtrait,mash} --kbase_collection KBASE_COLLECTION --source_ver SOURCE_VER [--env {CI,NEXT,APPDEV,PROD,NONE}]
                         [--load_ver LOAD_VER] [--root_dir ROOT_DIR] [--submit_job] [--force] [--source_file_ext SOURCE_FILE_EXT]

options:
  -h, --help            show this help message and exit

required named arguments:
  --tool {gtdb_tk,checkm2,microtrait,mash}
                        Name of tool to be executed. (e.g. gtdb_tk, checkm2, etc.)
  --kbase_collection KBASE_COLLECTION
                        KBase collection identifier name.
  --source_ver SOURCE_VER
                        Version of the source data, which should match the source directory in the collectionssource. (e.g. 207, 214 for GTDB, 2023.06 for GROW/PMI)

optional arguments:
  --env {CI,NEXT,APPDEV,PROD,NONE}
                        Environment containing the data to be processed. (default: PROD)
  --load_ver LOAD_VER   KBase load version (e.g. r207.kbase.1). (defaults to the source version)
  --root_dir ROOT_DIR   Root directory for the collections project. (default: /global/cfs/cdirs/kbase/collections)
  --submit_job          Submit job to slurm
  --force               Force overwrite of existing job directory
  --source_file_ext SOURCE_FILE_EXT
                        Select files from source data directory that match the given extension.
                        
'''

TOOLS_AVAILABLE = ['gtdb_tk', 'checkm2', 'microtrait', 'mash', 'eggnog']

NODE_TIME_LIMIT_DEFAULT = 5  # hours  # TODO: automatically calculate this based on tool execution time and NODE_THREADS
# Used as THREADS variable in the batch script which controls the number of parallel tasks per node
TASKS_PER_NODE_DEFAULT = 1

THREADS_PER_TOOL_RUN_DEFAULT = 32

# Task metadata - tool specific parameters for task generation and execution
# chunk_size is the quantity of genomes processed within each task (default is 5000)
#    for batch genome tools, such as gtdb_tk and checkm2, the chunk_size specifies the number of genomes grouped
#    together for processing in a batch by the tool
#    for single genome tools, such as microtrait and mash, the chunk_size is the number of genomes to process in a
#    serial manner
# exe_time is the estimated execution time for a single task (default is 60 minutes)
# threads_per_tool_run is the number of threads to use for each tool execution (default is 32)
# tasks_per_node is the number of parallel tasks to run on a node (default is 1)
# node_time_limit is the time limit for the node we reserved for the task (default is 5 hours)
# if no specific metadata is provided for a tool, the default values are used.
TASK_META = {'gtdb_tk': {'chunk_size': 1000, 'exe_time': 65, 'tasks_per_node': 4},
             'eggnog': {'chunk_size': 100, 'exe_time': 15, 'node_time_limit': 0.5},  # Memory intensive tool - reserve more nodes with less node reservation time
             'default': {'chunk_size': 5000, 'exe_time': 60}}
MAX_NODE_NUM = 100  # maximum number of nodes to use


REGISTRY = 'ghcr.io/kbase/collections'
VERSION_FILE = 'versions.yaml'
COMPUTE_TOOLS_DIR = '../../compute_tools'  # relative to task_generator.py

# volume name for the Docker containers (the internal container ref data mount directory)
TOOL_IMG_VOLUME_NAME = '/reference_data'

LIBRARY_DIR = 'libraries'  # subdirectory for the library files


def _retrieve_tool_volume(tool, root_dir):
    # Retrieve the volume mapping for the specified tool.

    current_dir = os.path.dirname(os.path.abspath(__file__))
    compute_tools_dir = os.path.join(current_dir, COMPUTE_TOOLS_DIR)
    version_file = os.path.join(compute_tools_dir, tool, VERSION_FILE)
    ref_db_version = extract_latest_reference_db_version(version_file)

    if not ref_db_version:
        # No reference database path needed for the tool (microtrait, mash).
        return dict()

    ref_db_path_abs = os.path.join(root_dir, LIBRARY_DIR, tool, ref_db_version)
    return {ref_db_path_abs: TOOL_IMG_VOLUME_NAME}


def _pull_image(image_str, job_dir):
    """
    Pulls the specified Shifter image from the specified registry.
    """

    print(f"Fetching Shifter image {image_str}...")
    sp_std_out_file, sp_std_err_file, sp_exit_code = tf_common.run_nersc_command(
        ["shifterimg", "pull", image_str], job_dir, log_file_prefix='shifterimg_pull')

    with open(sp_std_out_file, "r") as std_out, open(sp_std_err_file, "r") as std_err:
        sp_std_out = std_out.read()

        if 'FAILURE' in sp_std_out:
            raise ValueError(f"Error pulling Shifter image {image_str}.\n"
                             f"Standard output: {sp_std_out}\n"
                             f"Standard error: {std_err.read()}")


def _write_to_file(file_path, content):
    """
    Writes the specified content to the specified file. File is overwritten if it already exists.
    """
    with open(file_path, 'w') as file:
        file.write(content)


def _fetch_image(registry, tool, job_dir):
    """
    Fetches the specified Shifter image if it is not already present on the system.
    """

    current_dir = os.path.dirname(os.path.abspath(__file__))
    compute_tools_dir = os.path.join(current_dir, COMPUTE_TOOLS_DIR)
    version_file = os.path.join(compute_tools_dir, tool, VERSION_FILE)
    tool_img_ver = extract_latest_version(version_file)
    tool_img_tag = f'{tool}_{tool_img_ver}'
    image_str = f'{registry}:{tool_img_tag}'

    # Check if the image is already present on the system
    si_std_out_file, si_std_err_file, si_exit_code = tf_common.run_nersc_command(
        ["shifterimg", "images"], job_dir, log_file_prefix='shifterimg_images')

    with open(si_std_out_file, "r") as f:
        si_std_out = f.read()

    images = si_std_out.split("\n")
    for image in images:
        parts = image.split()
        if len(parts) != 6:
            continue
        if parts[5] == image_str:
            print(f"Shifter image {tool_img_tag} from registry {registry} already exists.")
            return parts[5]

    # Pull the image from the registry
    _pull_image(image_str, job_dir)

    return image_str


def _create_shifter_wrapper(job_dir, image_str):
    """
    Creates the Shifter wrapper script.
    """

    # The content of the Shifter wrapper script
    shifter_wrapper = f'''#!/bin/bash

image={image_str}

if [ $# -lt 1 ]; then
    echo "Error: Missing command argument."
    echo "Usage: shifter_wrapper.sh your-command-arguments"
    exit 1
fi

command="$@"
echo "Running shifter --image=$image $command"

cd {job_dir}
shifter --image=$image $command'''

    wrapper_file = os.path.join(job_dir, tf_common.WRAPPER_FILE)
    _write_to_file(wrapper_file, shifter_wrapper)

    os.chmod(wrapper_file, 0o777)

    return wrapper_file


def _create_genome_id_file(genome_ids, genome_id_file):
    """
    Create a tab-separated values (TSV) file with a list of genome IDs.
    """

    content = "genome_id\n" + "\n".join(genome_ids)
    _write_to_file(genome_id_file, content)


def _create_task_list(
        env: str,
        kbase_collection: str,
        source_ver: str,
        load_ver: str,
        tool: str,
        wrapper_file: str,
        job_dir: str,
        root_dir: str,
        source_file_ext: str = 'genomic.fna.gz'):
    """
    Create task list file (tasks.txt)
    """
    source_data_dir = make_collection_source_dir(root_dir, env, kbase_collection, source_ver)
    genome_ids = [path for path in os.listdir(source_data_dir) if
                  os.path.isdir(os.path.join(source_data_dir, path))]

    chunk_size = TASK_META.get(tool, TASK_META['default'])['chunk_size']
    threads_per_tool_run = TASK_META.get(tool, TASK_META['default']).get('threads_per_tool_run', THREADS_PER_TOOL_RUN_DEFAULT)
    genome_ids_chunks = [genome_ids[i: i + chunk_size] for i in range(0, len(genome_ids), chunk_size)]

    vol_mounts = _retrieve_tool_volume(tool, root_dir)

    task_list = '#!/usr/bin/env bash\n'
    for idx, genome_ids_chunk in enumerate(genome_ids_chunks):
        task_list += wrapper_file

        genome_id_file = os.path.join(job_dir, f'genome_id_{idx}.tsv')
        _create_genome_id_file(genome_ids_chunk, genome_id_file)

        env_vars = {'TOOLS': tool,
                    'ENV': env,
                    'KBASE_COLLECTION': kbase_collection,
                    'SOURCE_VER': source_ver,
                    'LOAD_VER': load_ver,
                    'ROOT_DIR': root_dir,
                    'NODE_ID': f'job_{idx}',
                    'GENOME_ID_FILE': genome_id_file,
                    'THREADS_PER_TOOL_RUN': threads_per_tool_run,
                    'SOURCE_FILE_EXT': source_file_ext}

        for mount_vol, docker_vol in vol_mounts.items():
            task_list += f''' --volume={mount_vol}:{docker_vol} '''

        for env_var, env_val in env_vars.items():
            task_list += f''' --env {env_var}={env_val} '''

        task_list += f'''--entrypoint\n'''

    task_list_file = os.path.join(job_dir, tf_common.TASK_FILE)
    _write_to_file(task_list_file, task_list)

    return task_list_file, len(genome_ids_chunks)


def _cal_node_num(tool, n_jobs):
    """
    Calculate the number of nodes required for the task
    """

    tool_exe_time = TASK_META.get(tool, TASK_META['default'])['exe_time']
    max_jobs_per_node = max(_get_node_time_limit(tool) * 60 // tool_exe_time, 1) * _get_node_task_count(tool)

    num_nodes = math.ceil(n_jobs / max_jobs_per_node)

    if num_nodes > MAX_NODE_NUM:
        raise ValueError(f"The number of nodes required ({num_nodes}) is greater than the maximum "
                         f"number of nodes allowed ({MAX_NODE_NUM}).")

    return num_nodes


def _get_node_task_count(tool: str) -> int:
    """
    Get the number of parallel tasks to run on a node
    """

    return TASK_META.get(tool, TASK_META['default']).get('tasks_per_node', TASKS_PER_NODE_DEFAULT)


def _get_node_time_limit(tool: str) -> float:
    """
    Get the time limit for the node we reserved for the task

    By default, we set the time limit to NODE_TIME_LIMIT (5 hours).
    If in TASK_META, the tool has a different time limit (node_time_limit), we will use that.
    """

    return TASK_META.get(tool, TASK_META['default']).get('node_time_limit', NODE_TIME_LIMIT_DEFAULT)


def _float_to_time(float_hours: float) -> str:
    """
    Convert a floating point number of hours to a time string in the format HH:MM:SS
    """
    hours = int(float_hours)
    decimal_part = float_hours - hours
    minutes = int(decimal_part * 60)
    seconds = int(decimal_part * 3600) % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _create_batch_script(job_dir, task_list_file, n_jobs, tool):
    """
    Create the batch script (submit_taskfarmer.sl) for job submission
    """

    node_num = _cal_node_num(tool, n_jobs)

    batch_script = f'''#!/bin/sh

#SBATCH -N {node_num + 1} -c 256
#SBATCH -q regular
#SBATCH --time={_float_to_time(_get_node_time_limit(tool))}
#SBATCH -C cpu

module load taskfarmer

cd {job_dir}
export THREADS={_get_node_task_count(tool)}

runcommands.sh {task_list_file}'''

    batch_script_file = os.path.join(job_dir, tf_common.BATCH_SCRIPT)
    _write_to_file(batch_script_file, batch_script)

    return batch_script_file


def _create_job_dir(job_dir, destroy_old_job_dir=False):
    """
    Create the job directory. If destroy_old_job_dir is True, recreate the job directory.
    """

    if os.path.exists(job_dir) and destroy_old_job_dir:
        print(f'removing job dir {job_dir}')
        shutil.rmtree(job_dir, ignore_errors=True)

    os.makedirs(job_dir, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(
        description='PROTOTYPE - Create the required documents/scripts for the TaskFarmer Workflow Manager'
                    'and provide the option to execute tasks.')
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional arguments')

    # Required flag arguments
    required.add_argument('--tool', required=True, type=str, choices=TOOLS_AVAILABLE,
                          help='Name of tool to be executed. (e.g. gtdb_tk, checkm2, etc.)')

    required.add_argument(f'--{loader_common_names.KBASE_COLLECTION_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.KBASE_COLLECTION_DESCR)

    required.add_argument(f'--{loader_common_names.SOURCE_VER_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.SOURCE_VER_DESCR)

    # Optional arguments
    optional.add_argument(
        f"--{loader_common_names.ENV_ARG_NAME}",
        type=str,
        choices=loader_common_names.KB_ENV + [loader_common_names.DEFAULT_ENV],
        default='PROD',
        help="Environment containing the data to be processed. (default: PROD)",
    )

    optional.add_argument(f'--{loader_common_names.LOAD_VER_ARG_NAME}', type=str,
                          help=loader_common_names.LOAD_VER_DESCR + ' (defaults to the source version)')
    optional.add_argument(
        f'--{loader_common_names.ROOT_DIR_ARG_NAME}',
        type=str,
        default=loader_common_names.ROOT_DIR,
        help=f'{loader_common_names.ROOT_DIR_DESCR} (default: {loader_common_names.ROOT_DIR})'
    )
    optional.add_argument('--submit_job', action='store_true', help='Submit job to slurm')
    optional.add_argument('--force', action='store_true', help='Force overwrite of existing job directory')
    optional.add_argument('--source_file_ext', type=str, default='.fa',
                          help='Select files from source data directory that match the given extension.')
    args = parser.parse_args()

    tool = args.tool
    env = getattr(args, loader_common_names.ENV_ARG_NAME)
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    source_ver = getattr(args, loader_common_names.SOURCE_VER_ARG_NAME)
    load_ver = getattr(args, loader_common_names.LOAD_VER_ARG_NAME)
    root_dir = getattr(args, loader_common_names.ROOT_DIR_ARG_NAME)
    if not load_ver:
        load_ver = source_ver

    source_data_dir = make_collection_source_dir(root_dir, env, kbase_collection, source_ver)
    source_file_ext = args.source_file_ext

    try:
        task_mgr = TFTaskManager(kbase_collection,
                                 load_ver,
                                 env,
                                 tool,
                                 source_data_dir,
                                 args.force,
                                 root_dir=root_dir)
    except PreconditionError as e:
        raise ValueError(f'Error submitting job:\n{e}\n'
                         f'Please use the --force flag to overwrite the previous run.') from e

    job_dir = task_mgr.job_dir
    _create_job_dir(job_dir, destroy_old_job_dir=args.force)

    image_str = _fetch_image(REGISTRY, tool, job_dir)
    wrapper_file = _create_shifter_wrapper(job_dir, image_str)
    task_list_file, n_jobs = _create_task_list(
        env,
        kbase_collection,
        source_ver,
        load_ver,
        tool,
        wrapper_file,
        job_dir,
        root_dir,
        source_file_ext=source_file_ext)

    batch_script = _create_batch_script(job_dir, task_list_file, n_jobs, tool)

    if args.submit_job:
        try:
            task_mgr.submit_job()
        except PreconditionError as e:
            raise ValueError(f'Error submitting job:\n{e}\n'
                             f'Please use the --force flag to overwrite the previous run.') from e
    else:
        print(f'Please go to Job Directory: {job_dir} and submit the batch script: {batch_script} to the scheduler.')


if __name__ == "__main__":
    main()
