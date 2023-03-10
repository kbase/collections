import argparse
import datetime
import math
import os
import shutil
import subprocess

import jsonlines

from src.loaders.common import loader_common_names

'''

Create the required documents and scripts for the TaskFarmer Workflow Manager and provide the option to execute tasks.

usage: task_generator.py [-h] --tool {gtdb_tk} --kbase_collection
                         KBASE_COLLECTION --load_ver LOAD_VER
                         --source_data_dir SOURCE_DATA_DIR
                         [--root_dir ROOT_DIR] [--image_tag IMAGE_TAG]
                         [--use_cached_image] [--submit_job] [--force]

optional arguments:
  -h, --help            show this help message and exit

required named arguments:
  --tool {gtdb_tk}      Name of tool to be executed. (e.g. gtdb_tk, checkm2,
                        etc.)
  --kbase_collection KBASE_COLLECTION
                        KBase collection identifier name (e.g. GTDB).
  --load_ver LOAD_VER   KBase load version (e.g. r207.kbase.1).
  --source_data_dir SOURCE_DATA_DIR
                        Source data (genome files) directory. (e.g. /global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207

optional arguments:
  --root_dir ROOT_DIR   Root directory for the collections project. (default: /global/cfs/cdirs/kbase/collections)
  --image_tag IMAGE_TAG
                        Docker/Shifter image tag. (default: latest)
  --use_cached_image    Use an existing image without pulling
  --submit_job          Submit job to slurm
  --force               Force overwrite of existing job directory
'''

TOOLS_AVAILABLE = ['gtdb_tk']  # TODO: fix checkm2 container bug

# estimated execution time (in minutes) for each tool to process a chunk of data
TASK_META = {'checkm2': {'chunk_size': 5000, 'exe_time': 60},
             'gtdb_tk': {'chunk_size': 1000, 'exe_time': 90}}
NODE_TIME_LIMIT = 10  # hours
MAX_NODE_NUM = 100  # maximum number of nodes to use

REGISTRY = 'tiangu01'  # public Docker Hub registry to pull images from

# directory containing the unarchived GTDB-Tk reference data
# download data following the instructions provided on
# https://ecogenomics.github.io/GTDBTk/installing/index.html#gtdb-tk-reference-data
GTDBTK_DATA_PATH = '/global/cfs/cdirs/kbase/collections/libraries/gtdb_tk/release207_v2'

# DIAMOND database CheckM2 relies on
# download data following the instructions provided on https://github.com/chklovski/CheckM2#database
CHECKM2_DB = '/global/cfs/cdirs/kbase/collections/libraries/CheckM2_database'

# volume mapping for the Docker containers
TOOL_VOLUME_MAP = {'checkm2': {CHECKM2_DB: '/CheckM2_database'},
                   'gtdb_tk': {GTDBTK_DATA_PATH: '/gtdbtk_reference_data'}}

# Docker image tags for the tools
DEFAULT_IMG_TAG = 'latest'

# directory containing the TaskFarmer job scripts under the root directory
TASKFARMER_JOB_DIR = 'task_farmer_jobs'
TASK_INFO_FILE = 'task_info.jsonl'  # file containing the information of each task


def _run_command(command, job_dir, log_file_prefix='', check_return_code=True):
    """
    Runs the specified command and captures its standard output and standard error.
    """
    log_dir = os.path.join(job_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    std_out_file = os.path.join(log_dir,
                                f'stdout_{log_file_prefix}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}')
    std_err_file = os.path.join(log_dir,
                                f'stderr_{log_file_prefix}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}')

    with open(std_out_file, "w") as std_out, open(std_err_file, "w") as std_err:
        p = subprocess.Popen(command, stdout=std_out, stderr=std_err, text=True)

    exit_code = p.wait()

    if check_return_code and exit_code != 0:
        with open(std_out_file, "r") as std_out, open(std_err_file, "r") as std_err:
            raise ValueError(f"Error running command '{command}'.\n"
                             f"Standard output: {std_out.read()}\n"
                             f"Standard error: {std_err.read()}")

    return std_out_file, std_err_file, exit_code


def _pull_image(image_str, job_dir):
    """
    Pulls the specified Shifter image from the specified registry.
    """

    print(f"Fetching Shifter image {image_str}...")
    sp_std_out_file, sp_std_err_file, sp_exit_code = _run_command(["shifterimg", "pull", image_str], job_dir,
                                                                  log_file_prefix='shifterimg_pull')

    with open(sp_std_out_file, "r") as std_out, open(sp_std_err_file, "r") as std_err:
        sp_std_out = std_out.read()

        if 'FAILURE' in sp_std_out:
            raise ValueError(f"Error pulling Shifter image {image_str}.\n"
                             f"Standard output: {sp_std_out}\n"
                             f"Standard error: {std_err.read()}")


def _fetch_image(registry, image_name, job_dir, tag='latest', force_pull=True):
    """
    Fetches the specified Shifter image if it is not already present on the system.

    When force_pull is set to True, the image is always pulled from the registry
    """
    image_str = f"{registry}/{image_name}:{tag}"

    if not force_pull:
        # Check if the image is already present on the system
        si_std_out_file, si_std_err_file, si_exit_code = _run_command(["shifterimg", "images"], job_dir,
                                                                      log_file_prefix='shifterimg_images')

        with open(si_std_out_file, "r") as f:
            si_std_out = f.read()

        images = si_std_out.split("\n")
        for image in images:
            parts = image.split()
            if len(parts) != 6:
                continue
            if parts[5] == image_str:
                print(f"Shifter image {image_name}:{tag} from registry {registry} already exists.")
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

    wrapper_file = os.path.join(job_dir, 'shifter_wrapper.sh')
    with open(wrapper_file, "w") as f:
        f.write(shifter_wrapper)

    os.chmod(wrapper_file, 0o777)

    return wrapper_file


def _create_genome_id_file(genome_ids, genome_id_file):
    """
    Create a tab-separated values (TSV) file with a list of genome IDs.
    """
    with open(genome_id_file, 'w') as f:
        f.write("genome_id\n")
        for genome_id in genome_ids:
            f.write(f"{genome_id}\n")


def _create_task_list(source_data_dir, kbase_collection, load_ver, tool, wrapper_file, job_dir, root_dir,
                      threads=256, program_threads=256, source_file_ext='genomic.fna.gz'):
    """
    Create task list file (tasks.txt)
    """
    genome_ids = [path for path in os.listdir(source_data_dir) if
                  os.path.isdir(os.path.join(source_data_dir, path))]

    chunk_size = TASK_META[tool]['chunk_size']
    genome_ids_chunks = [genome_ids[i: i + chunk_size] for i in range(0, len(genome_ids), chunk_size)]

    vol_mounts = TOOL_VOLUME_MAP.get(tool, {})

    task_list = '#!/usr/bin/env bash\n'
    for idx, genome_ids_chunk in enumerate(genome_ids_chunks):
        task_list += wrapper_file

        genome_id_file = os.path.join(job_dir, f'genome_id_{idx}.tsv')
        _create_genome_id_file(genome_ids_chunk, genome_id_file)

        env_vars = {'TOOLS': tool, 'LOAD_VER': load_ver, 'SOURCE_DATA_DIR': source_data_dir,
                    'KBASE_COLLECTION': kbase_collection, 'ROOT_DIR': root_dir,
                    'NODE_ID': f'job_{idx}', 'GENOME_ID_FILE': genome_id_file,
                    'THREADS': threads, 'PROGRAM_THREADS': program_threads, 'SOURCE_FILE_EXT': source_file_ext}

        for mount_vol, docker_vol in vol_mounts.items():
            task_list += f''' --volume={mount_vol}:{docker_vol} '''

        for env_var, env_val in env_vars.items():
            task_list += f'''--env {env_var}={env_val} '''

        task_list += f'''--entrypoint\n'''

    task_list_file = os.path.join(job_dir, 'tasks.txt')
    with open(task_list_file, "w") as f:
        f.write(task_list)

    return task_list_file, len(genome_ids_chunks)


def _cal_node_num(tool, n_jobs):
    """
    Calculate the number of nodes required for the task
    """

    tool_exe_time = TASK_META[tool]['exe_time']
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
export THREADS=32

runcommands.sh {task_list_file}'''

    batch_script_file = os.path.join(job_dir, 'submit_taskfarmer.sl')
    with open(batch_script_file, "w") as f:
        f.write(batch_script)

    return batch_script_file


def _create_job_dir(root_dir, tool, kbase_collection, load_ver, force=False):
    """
    Create the job directory

    Job directory structure: <root_dir>/<TASKFARMER_JOB_DIR>/<kbase_collection>_<load_ver>_<tool>
    """

    job_dir = os.path.join(root_dir, TASKFARMER_JOB_DIR, f'{kbase_collection}_{load_ver}_{tool}')

    if os.path.exists(job_dir) and force:
        shutil.rmtree(job_dir)

    os.makedirs(job_dir, exist_ok=True)

    return job_dir


def _append_task_info(root_dir, task_info):
    """
    Append the task information to the task info file
    """

    task_info_file = os.path.join(root_dir, TASKFARMER_JOB_DIR, TASK_INFO_FILE)

    # Append the new record to the file
    with jsonlines.open(task_info_file, mode='a') as writer:
        writer.write(task_info)


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

    required.add_argument(f'--{loader_common_names.LOAD_VER_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.LOAD_VER_DESCR)

    required.add_argument('--source_data_dir', required=True, type=str,
                          help='Source data (genome files) directory. '
                               '(e.g. /global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207')

    # Optional arguments
    optional.add_argument('--root_dir', type=str, default=loader_common_names.ROOT_DIR,
                          help=f'Root directory for the collections project. (default: {loader_common_names.ROOT_DIR})')
    optional.add_argument('--image_tag', type=str, default=DEFAULT_IMG_TAG,
                          help=f'Docker/Shifter image tag. (default: {DEFAULT_IMG_TAG})')
    optional.add_argument('--use_cached_image', action='store_true',
                          help='Use an existing image without pulling')
    optional.add_argument('--submit_job', action='store_true', help='Submit job to slurm')
    optional.add_argument('--force', action='store_true', help='Force overwrite of existing job directory')

    args = parser.parse_args()

    tool = args.tool
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    load_ver = getattr(args, loader_common_names.LOAD_VER_ARG_NAME)
    source_data_dir = args.source_data_dir
    root_dir = args.root_dir

    current_datetime = datetime.datetime.now()
    job_dir = _create_job_dir(root_dir, tool, kbase_collection, load_ver, force=args.force)

    image_str = _fetch_image(REGISTRY, tool, job_dir, tag=args.image_tag, force_pull=not args.use_cached_image)
    wrapper_file = _create_shifter_wrapper(job_dir, image_str)

    task_list_file, n_jobs = _create_task_list(source_data_dir, kbase_collection, load_ver, tool, wrapper_file, job_dir,
                                               root_dir)

    batch_script = _create_batch_script(job_dir, task_list_file, n_jobs, tool)

    job_id = None
    if args.submit_job:
        os.chdir(job_dir)
        std_out_file, std_err_file, exit_code = _run_command(['sbatch', os.path.join(job_dir, 'submit_taskfarmer.sl')],
                                                             job_dir, log_file_prefix='sbatch_submit')
        with open(std_out_file, "r") as f:
            sbatch_out = f.read().strip()
            job_id = sbatch_out.split(' ')[-1]
            print(f'Job submitted to slurm.\n{sbatch_out}')
    else:
        print(f'Please go to Job Directory: {job_dir} and submit the batch script: {batch_script} to the scheduler.')

    if job_id:
        task_info = {'kbase_collection': kbase_collection, 'load_ver': load_ver, 'tool': tool,
                     'job_id': job_id, 'job_start_time': current_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                     'source_data_dir': source_data_dir}
        _append_task_info(root_dir, task_info)


if __name__ == "__main__":
    main()
