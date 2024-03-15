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
                        
Note: Based on our experiment with GTDB-Tk, we have determined that the optimal chunk size is 1000 genomes.
With 4 batches running in parallel, each using 32 cores, it takes around 50 minutes to process a single chunk.
To reflect this, we have set the "threads" and "program_threads" parameters in "_create_task_list" to 32, 
indicating that each batch will use 32 cores. We have also set the execution time for GTDB-Tk to 65 minutes, 
with some additional buffer time. To allow for a sufficient number of batches to be run within a given time limit, 
we have set the "NODE_TIME_LIMIT" to 5 hours. With these settings, we expect to be able to process up to 16 batches, 
or 16,000 genomes, per node within the 5-hour time limit. We plan to make these parameters configurable based on 
the specific tool being used. After conducting performance tests, we found that utilizing 32 cores per batch and 
running 4 batches in parallel per NERSC node resulted in optimal performance, despite each node having a total of 
256 cores.
'''

TOOLS_AVAILABLE = ['gtdb_tk', 'checkm2', 'microtrait', 'mash', 'eggnog']

# estimated execution time (in minutes) for each tool to process a chunk of data
TASK_META = {'gtdb_tk': {'chunk_size': 1000, 'exe_time': 65},
             'eggnog': {'chunk_size': 1000, 'exe_time': 65},  # TODO: update this value after performance testing
             'default': {'chunk_size': 5000, 'exe_time': 60}}
NODE_TIME_LIMIT = 5  # hours  # TODO: automatically calculate this based on tool execution time and NODE_THREADS
MAX_NODE_NUM = 100  # maximum number of nodes to use
# The THREADS variable controls the number of parallel tasks per node
# TODO: make this configurable based on tool used. At present, we have set the value to 4 for optimal performance with GTDB-Tk.
NODE_THREADS = 4

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
        threads: int = 32,
        program_threads: int = 32,
        source_file_ext: str = 'genomic.fna.gz'):
    """
    Create task list file (tasks.txt)

    threads: the total number of threads to use per node
    program_threads: number of threads to use per task
    For instance, if "threads" is set to 128 and "program_threads" to 32, then each task will run 4 batches in parallel.

    We have chosen to use "taskfarmer" for parallelization, which means that "threads" and "program_threads" should
    have the same value. This ensures that parallelization only happens between tasks, and not within them.

    TODO: make threads/program_threads configurable based on tool used. However, for the time being, we have set
    these parameters to 32 and , since this value has produced the highest throughput in our experiments.
    """
    source_data_dir = make_collection_source_dir(root_dir, env, kbase_collection, source_ver)
    genome_ids = [path for path in os.listdir(source_data_dir) if
                  os.path.isdir(os.path.join(source_data_dir, path))]

    chunk_size = TASK_META.get(tool, TASK_META['default'])['chunk_size']
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
                    'THREADS': threads,
                    'PROGRAM_THREADS': program_threads,
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
    jobs_per_node = NODE_TIME_LIMIT * 60 // tool_exe_time

    num_nodes = math.ceil(n_jobs / jobs_per_node)

    if num_nodes > MAX_NODE_NUM:
        raise ValueError(f"The number of nodes required ({num_nodes}) is greater than the maximum "
                         f"number of nodes allowed ({MAX_NODE_NUM}).")

    return num_nodes


def _create_batch_script(job_dir, task_list_file, n_jobs, tool):
    """
    Create the batch script (submit_taskfarmer.sl) for job submission
    """

    node_num = _cal_node_num(tool, n_jobs)

    batch_script = f'''#!/bin/sh

#SBATCH -N {node_num + 1} -c 256
#SBATCH -q regular
#SBATCH --time={NODE_TIME_LIMIT}:00:00
#SBATCH -C cpu

module load taskfarmer

cd {job_dir}
export THREADS={NODE_THREADS}

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
