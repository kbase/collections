import argparse
import datetime
import os
import subprocess

from src.loaders.common import loader_common_names


'''

Create required documents and scripts for the TaskFarmer Workflow Manager.

usage: task_generator.py [-h] --tool {checkm2,gtdb_tk} --kbase_collection KBASE_COLLECTION --load_ver LOAD_VER --source_data_dir SOURCE_DATA_DIR
                         [--root_dir ROOT_DIR] [--submit_job] [--no_submit_job]

optional arguments:
  -h, --help            show this help message and exit

required named arguments:
  --tool {checkm2,gtdb_tk}
                        Name of tool to be executed. (e.g. gtdb_tk, checkm2, etc.)
  --kbase_collection KBASE_COLLECTION
                        KBase collection identifier name (e.g. GTDB).
  --load_ver LOAD_VER   KBase load version (e.g. r207.kbase.1).
  --source_data_dir SOURCE_DATA_DIR
                        Source data (genome files) directory. (e.g. /global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207

optional arguments:
  --root_dir ROOT_DIR   Root directory for the collections project. (default: /global/cfs/cdirs/kbase/collections)
  --submit_job          Submit job to slurm
  --no_submit_job       Do not submit job to slurm
'''

TOOLS_AVAILABLE = ['checkm2', 'gtdb_tk']
CHUNK_SIZE = 1000

REGISTRY = 'tiangu01'  # public Docker Hub registry to pull images from

# directory containing the unarchived GTDB-Tk reference data
GTDBTK_DATA_PATH = '/global/cfs/cdirs/kbase/collections/libraries/gtdb_tk/release207_v2'

# DIAMOND database CheckM2 relies on
CHECKM2_DB = '/global/cfs/cdirs/kbase/collections/libraries/CheckM2_database'


def _run_command(command, check_return_code=True):
    """
    Runs the specified command and captures its standard output and standard error.
    """

    process = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    std_out = process.stdout.decode("utf-8").strip()
    std_err = process.stderr.decode("utf-8").strip()
    return_code = process.returncode

    if check_return_code and return_code != 0:
        raise Exception(f"Error running command '{command}'.\n"
                        f"Standard output: {std_out}\n"
                        f"Standard error: {std_err}")

    return std_out, std_err, process.returncode


def _fetch_image(registry, image_name, ver='latest'):
    """
    Fetches the specified Shifter image, if it's not already present on the system.
    """
    image_str = f"{registry}/{image_name}:{ver}"

    # Check if the image is already present on the system
    std_out, std_err, returncode = _run_command(["shifterimg", "images"])

    images = std_out.split("\n")
    for image in images:
        parts = image.split()
        if len(parts) != 6:
            continue
        if parts[5] == image_str:
            print(f"Shifter image {image_name}:{ver} from registry {registry} already exists.")
            return parts[5]

    # Pull the image from the registry
    print(f"Fetching Shifter image {image_str}...")
    std_out, std_err, returncode = _run_command(["shifterimg", "pull", image_str])

    if 'FAILURE' in std_out:
        raise ValueError(f"Error fetching Shifter image {image_str}.")

    return image_str


def _create_shifter_wrapper(job_dir, image_str):
    """
    Creates the Shifter wrapper script.
    """

    # The content of the Shifter wrapper script
    shifter_wrapper = "#!/bin/bash\n\n"
    shifter_wrapper += f"image={image_str}\n\n"
    shifter_wrapper += "if [ $# -lt 1 ]; then\n"
    shifter_wrapper += '    echo "Error: Missing command argument."\n'
    shifter_wrapper += '    echo "Usage: shifter_wrapper.sh your-command-arguments"\n'
    shifter_wrapper += "    exit 1\n"
    shifter_wrapper += "fi\n\n"
    shifter_wrapper += "command=\"$@\"\n\n"
    shifter_wrapper += "echo \"Running shifter --image=$image $command\"\n\n"
    shifter_wrapper += f"cd $HOME\n"
    shifter_wrapper += "shifter --image=$image $command\n"  # Run the command in the Shifter environment

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


def _create_task_list(source_data_dir, kbase_collection, load_ver, tool, wrapper_file, job_dir, root_dir):
    """
    Create task list file (tasks.txt)
    """
    genome_ids = [path for path in os.listdir(source_data_dir) if
                  os.path.isdir(os.path.join(source_data_dir, path))]

    genome_ids_chunks = [genome_ids[i: i + CHUNK_SIZE] for i in range(0, len(genome_ids), CHUNK_SIZE)]

    task_list = '#!/usr/bin/env bash\n'
    for idx, genome_ids_chunk in enumerate(genome_ids_chunks):

        task_list += wrapper_file

        genome_id_file = os.path.join(job_dir, f'genome_id_{idx}.tsv')
        _create_genome_id_file(genome_ids_chunk, genome_id_file)

        if tool == 'checkm2':
            task_list += f'	--volume={CHECKM2_DB}:/CheckM2_database --volume={root_dir}:/checkm2_root_dir ' \
                         f'conda run -n checkm2-1.0.0 ' \
                         f'python collections/src/loaders/genome_collection/compute_genome_attribs.py ' \
                         f'--tools checkm2 '
        elif tool == 'gtdb_tk':
            task_list += f' --volume={GTDBTK_DATA_PATH}:/gtdbtk_reference_data ' \
                         f'conda run -n gtdbtk-2.1.1 ' \
                         f'python collections/src/loaders/genome_collection/compute_genome_attribs.py ' \
                         f'--tools gtdb_tk '
        else:
            raise ValueError(f'Unexpected tool {tool}')

        task_list += f'--load_ver {load_ver} --source_data_dir {source_data_dir} ' \
                     f'--kbase_collection {kbase_collection} --root_dir {root_dir} ' \
                     f'--threads 128 --program_threads 128 --node_id job_{idx} ' \
                     f'--debug --source_file_ext genomic.fna.gz --genome_id_file {genome_id_file}\n'

    task_list_file = os.path.join(job_dir, 'tasks.txt')
    with open(task_list_file, "w") as f:
        f.write(task_list)

    return task_list_file, len(genome_ids_chunks)


def _create_batch_script(job_dir, task_list_file, n_jobs):
    """
    Create the batch script (submit_taskfarmer.sl) for job submission
    """
    batch_script = '#!/bin/sh\n'
    batch_script += f'#SBATCH -N {n_jobs + 1} -c 64\n'
    batch_script += '#SBATCH -q regular\n'
    batch_script += '#SBATCH --time=2:00:00\n'
    batch_script += '#SBATCH --time-min=0:30:00\n'
    batch_script += '#SBATCH -C cpu\n\n'
    batch_script += f'cd {job_dir}\n'
    batch_script += 'export THREADS=32\n\n'
    batch_script += f'runcommands.sh {task_list_file}'

    batch_script_file = os.path.join(job_dir, 'submit_taskfarmer.sl')
    with open(batch_script_file, "w") as f:
        f.write(batch_script)

    return batch_script_file


def main():
    parser = argparse.ArgumentParser(
        description='PROTOTYPE - Create the required documents/scripts for the TaskFarmer Workflow Manager.')
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

    optional.add_argument('--submit_job', action='store_true', help='Submit job to slurm')
    optional.add_argument('--no_submit_job', dest='submit_job', action='store_false', help='Do not submit job to slurm')

    args = parser.parse_args()

    tool = args.tool
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    load_ver = getattr(args, loader_common_names.LOAD_VER_ARG_NAME)
    source_data_dir = args.source_data_dir
    root_dir = args.root_dir

    current_datetime = datetime.datetime.now()
    job_dir = os.path.join(root_dir, 'task_farmer_jobs', f'{tool}_{current_datetime.strftime("%Y_%m_%d_%H_%M_%S")}')
    os.makedirs(job_dir, exist_ok=True)

    image_str = _fetch_image(REGISTRY, tool)
    wrapper_file = _create_shifter_wrapper(job_dir, image_str)

    task_list_file, n_jobs = _create_task_list(source_data_dir, kbase_collection, load_ver, tool, wrapper_file, job_dir,
                                               root_dir)

    batch_script = _create_batch_script(job_dir, task_list_file, n_jobs)

    if args.submit_job:
        std_out, std_err, returncode = _run_command(['sbatch', os.path.join(job_dir, 'submit_taskfarmer.sl')])
        print(f'Job submitted to slurm.\n{std_out.strip()}')
    else:
        print(f'Please go to Job Directory: {job_dir} and submit the batch script: {batch_script} to the scheduler.')


if __name__ == "__main__":
    main()
