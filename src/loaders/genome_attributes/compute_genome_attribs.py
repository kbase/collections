"""
PROTOTYPE

Before calling this script, the required input files for each tooling should be already downloaded at
[root_dir]/sourcedata/[source]/[release_ver] using the `ncbi_downloader.py` script.

This script will compute and save result files to [root_dir/collectionsdata/[kbase_collection]/[load_ver]/[tool_name].


usage: compute_genome_attribs.py [-h] --tools TOOLS [TOOLS ...] --load_ver LOAD_VER --source_data_dir SOURCE_DATA_DIR
                                 [--kbase_collection KBASE_COLLECTION] [--root_dir ROOT_DIR] [--threads THREADS]
                                 [--program_threads PROGRAM_THREADS] [--node_id NODE_ID] [--debug]
                                 [--genome_id_file GENOME_ID_FILE]

options:
  -h, --help            show this help message and exit

required named arguments:
  --tools TOOLS [TOOLS ...]
                        Tools to be executed. (e.g. gtdb_tk, checkm2, etc.)
  --load_ver LOAD_VER   KBase load version. (e.g. r207.kbase.1)
  --source_data_dir SOURCE_DATA_DIR
                        Source data (genome files) directory. (e.g.
                        /global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207

optional arguments:
  --kbase_collection KBASE_COLLECTION
                        KBase collection identifier name. (default: GTDB)
  --root_dir ROOT_DIR   Root directory.
  --threads THREADS     Total number of threads used by the script. (default: half of system cpu count)
  --program_threads PROGRAM_THREADS
                        Number of threads to execute a single tool command. threads / program_threads determines the
                        number of batches. (default: 32)
  --node_id NODE_ID     node ID for running job
  --debug               Debug mode.
  --genome_id_file GENOME_ID_FILE
                        tab separated file containing genome ids for the running job (requires 'genome_id' as the column
                        name)
  --source_file_ext SOURCE_FILE_EXT
                        Select files from source data directory that match the given extension.



e.g. python compute_genome_attribs.py --tools gtdb_tk checkm2 --load_ver r207.kbase.1 --source_data_dir /global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207 --debug

NOTE:
NERSC file structure for GTDB:
/global/cfs/cdirs/kbase/collections/collectionsdata/ -> [kbase_collection] -> [load_ver] -> [tool_name]
e.g.
/global/cfs/cdirs/kbase/collections/collectionsdata/GTDB -> r207.kbase.1 -> gtdb_tk -> batch_0_size_x_node_x -> result files
                                                                                    -> batch_1_size_x_node_x -> result files
                                                                         -> checkm2 -> batch_0_size_x_node_x -> result files
                                                                                    -> batch_1_size_x_node_x -> result files

Require defining the 'GTDBTK_DATA_PATH' environment variable to run GTDB_TK tool
e.g. export GTDBTK_DATA_PATH=/global/cfs/cdirs/kbase/collections/libraries/gtdb_tk/release207_v2
"""
import argparse
import itertools
import math
import multiprocessing
import os
import subprocess
import sys
import time
import uuid

import pandas as pd

from src.loaders.common import loader_common_names

SERIES_TOOLS = []  # Tools cannot be executed in parallel

# Fraction amount of system cores can be utilized
# (i.e. 0.5 - program will use 50% of total processors,
#       0.1 - program will use 10% of total processors)
SYSTEM_UTILIZATION = 0.5


def _find_genome_file(genome_id, file_ext, source_data_dir, exclude_file_name_substr=None, expected_file_count=1):
    genome_path = os.path.join(source_data_dir, genome_id)

    if not os.path.exists(genome_path):
        raise ValueError(f'Cannot find file directory for: {genome_id}')

    genome_files = [os.path.join(genome_path, f) for f in os.listdir(genome_path) if f.endswith(file_ext)]

    if exclude_file_name_substr:
        genome_files = [f for f in genome_files if
                        all(name_substr not in f for name_substr in exclude_file_name_substr)]

    if not genome_files or len(genome_files) != expected_file_count:
        print(f'Cannot retrieve target file(s). Please check download folder for genome: {genome_id}')
        return None

    return genome_files


def _run_command(command, debug=False, log_dir=''):
    # execute command.
    # if debug is set, write output of stdout and stderr to files (named as stdout and stderr) to log_dir

    if debug:
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(log_dir, 'stdout'), "w") as std_out, open(os.path.join(log_dir, 'stderr'),
                                                                         "w") as std_err:
            p = subprocess.Popen(command, stdout=std_out, stderr=std_err, text=True)
    else:
        p = subprocess.Popen(command)

    exit_code = p.wait()

    if exit_code != 0:
        raise ValueError(f'The command {command} failed with exit code {exit_code}')


def _create_batch_dir(work_dir, batch_number, size, node_id):
    # create working directory for each batch
    batch_dir = os.path.join(work_dir, f'batch_{batch_number}_size_{size}_node_{node_id}')
    os.makedirs(batch_dir, exist_ok=True)

    return batch_dir


def _run_gtdb_tk_steps(batch_file_path, work_dir, debug, genome_ids, program_threads):
    # run GTDB-TK classify_wf in steps
    # Classify genomes by placement in GTDB reference tree (identify -> align -> classify)
    start = time.time()

    # RUN gtdbtk identify step
    command = ['gtdbtk', 'identify',
               '--batchfile', batch_file_path,
               '--out_dir', os.path.join(work_dir, 'identify'),
               '--cpus', str(program_threads)]
    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    _run_command(command, debug=debug, log_dir=os.path.join(work_dir, 'identify_log'))
    end_identify_time = time.time()
    print(
        f'Used {round((end_identify_time - start) / 60, 2)} minutes to execute gtdbtk identify for '
        f'{len(genome_ids)} genomes')

    # RUN gtdbtk align step
    command = ['gtdbtk', 'align',
               '--identify_dir', os.path.join(work_dir, 'identify'),
               '--out_dir', os.path.join(work_dir, 'align'),
               '--cpus', str(program_threads)]
    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    _run_command(command, debug=debug, log_dir=os.path.join(work_dir, 'align_log'))
    end_align_time = time.time()
    print(
        f'Used {round((end_align_time - end_identify_time) / 60, 2)} minutes to execute gtdbtk align for '
        f'{len(genome_ids)} genomes')

    # RUN gtdbtk classify step
    command = ['gtdbtk', 'classify',
               '--batchfile', batch_file_path,
               '--align_dir', os.path.join(work_dir, 'align'),
               '--out_dir', os.path.join(work_dir, 'classify'),
               '--cpus', str(program_threads)]
    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    _run_command(command, debug=debug, log_dir=os.path.join(work_dir, 'classify_log'))
    end_classify_time = time.time()
    print(
        f'Used {round((end_classify_time - end_align_time) / 60, 2)} minutes to execute gtdbtk classify for '
        f'{len(genome_ids)} genomes')


def _run_gtdb_tk_classify_wf(batch_file_path, work_dir, debug, genome_ids, program_threads):
    # run GTDB-TK classify_wf workflow
    start = time.time()

    # RUN gtdbtk classify_wf
    command = ['gtdbtk', 'classify_wf',
               '--batchfile', batch_file_path,
               '--out_dir', work_dir,
               '--force',
               '--cpus', str(program_threads)]
    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    _run_command(command, debug=debug, log_dir=os.path.join(work_dir, 'classify_wf_log'))
    end_time = time.time()
    print(
        f'Used {round((end_time - start) / 60, 2)} minutes to execute gtdbtk classify_wf for '
        f'{len(genome_ids)} genomes')


def _map_tool_id_to_genome_id(tool_name, original_genome_id, genome_file_name, source_file_ext):
    """
    Construct a dictionary that associates tool-generated genome IDs with the original genome IDs used to create
    the _key field of a document.

    The method is specific to the tool being used, as each tool has its own logic for generating unique genome IDs.
    """

    if tool_name == 'checkm2':
        # CheckM2 uses the base name (without extension) of genome_file as the genome identifier
        # We know for checkM2 extension 'genomic.fna.gz' is consistently used for genome files

        base_name = genome_file_name.split(source_file_ext)[0]
        return {base_name: original_genome_id}

    elif tool_name == 'gtdb_tk':
        # When creating the batch file for GTDB-TK, we use the genome ID as the identifier for each genome.
        return {original_genome_id: original_genome_id}
    else:
        raise ValueError(f'the method for tool {tool_name} has not been implemented.')


def _create_genome_metadata_file(tool_genome_id_map, source_genome_file_map, genome_count, batch_dir):
    # create tab separated metadata file with tool generated genome identifier, original genome id and
    # source genome file info.

    if len(tool_genome_id_map) != genome_count:
        raise ValueError('Some genomes are absent from the genome metadata file')

    # create tool genome identifier metadata file
    genome_meta_file_path = os.path.join(batch_dir, loader_common_names.GENOME_METADATA_FILE)
    with open(genome_meta_file_path, "w") as meta_file:
        for tool_genome_identifier, genome_id in tool_genome_id_map.items():
            meta_file.write(f'{tool_genome_identifier}\t{genome_id}\t{source_genome_file_map.get(genome_id)}\n')


def gtdb_tk(genome_ids, work_dir, source_data_dir, debug, program_threads, batch_number,
            node_id, source_file_ext, run_steps=False):
    # NOTE: Require defining the 'GTDBTK_DATA_PATH' environment variable
    #       e.g. export GTDBTK_DATA_PATH=/global/cfs/cdirs/kbase/collections/libraries/gtdb_tk/release207_v2
    #       https://ecogenomics.github.io/GTDBTk/installing/index.html#gtdb-tk-reference-data
    #
    #       Ensure that Third-party software are on the system path.
    #       https://ecogenomics.github.io/GTDBTk/installing/index.html#installing-third-party-software

    failed_ids, size = list(), len(genome_ids)
    print(f'Start executing GTDB-TK for {size} genomes')

    batch_dir = _create_batch_dir(work_dir, batch_number, size, node_id)

    # create the batch file
    # tab separated in 2 columns (FASTA file, genome ID)
    batch_file_name = f'genome.fasta.list'
    batch_file_path = os.path.join(batch_dir, batch_file_name)
    tool_genome_id_map, source_genome_file_map = dict(), dict()
    with open(batch_file_path, "w") as batch_file:
        for genome_id in genome_ids:
            genome_file = _find_genome_file(genome_id, source_file_ext, source_data_dir,
                                            exclude_file_name_substr=['cds_from', 'rna_from', 'ERR'],
                                            expected_file_count=1)

            if genome_file:
                tool_genome_id_map.update(_map_tool_id_to_genome_id('gtdb_tk',
                                                                    genome_id,
                                                                    os.path.basename(genome_file[0]),
                                                                    source_file_ext))
                source_genome_file_map[genome_id] = genome_file[0]
                # According to GTDB, the batch file should be a two column file indicating the location of each genome
                # and the desired genome identifier
                batch_file.write(f'{genome_file[0]}\t{genome_id}\n')
    _create_genome_metadata_file(tool_genome_id_map, source_genome_file_map, len(genome_ids), batch_dir)
    if run_steps:
        _run_gtdb_tk_steps(batch_file_path, batch_dir, debug, genome_ids, program_threads)
    else:
        _run_gtdb_tk_classify_wf(batch_file_path, batch_dir, debug, genome_ids, program_threads)

    # TODO: inspect stdout for failed ids or do it in the parser program
    return failed_ids


def checkm2(genome_ids, work_dir, source_data_dir, debug, program_threads, batch_number, node_id, source_file_ext):
    # NOTE: require Python <= 3.9
    # Many checkm2 dependencies (e.g. scikit-learn=0.23.2, tensorflow, diamond, etc.) support Python version up to 3.9
    # TODO: run checkm2 tool via docker container

    failed_ids, size = list(), len(genome_ids)
    print(f'Start executing checkM2 for {len(genome_ids)} genomes')

    batch_dir = _create_batch_dir(work_dir, batch_number, size, node_id)
    tool_genome_id_map, source_genome_file_map = dict(), dict()
    # retrieve genomic.fna.gz files
    fna_files = list()
    for genome_id in genome_ids:
        genome_file = _find_genome_file(genome_id, source_file_ext, source_data_dir,
                                        exclude_file_name_substr=['cds_from', 'rna_from', 'ERR'],
                                        expected_file_count=1)

        if genome_file:
            tool_genome_id_map.update(_map_tool_id_to_genome_id('checkm2',
                                                                genome_id,
                                                                os.path.basename(genome_file[0]),
                                                                source_file_ext))
            source_genome_file_map[genome_id] = genome_file[0]
            fna_files.append(str(genome_file[0]))

    _create_genome_metadata_file(tool_genome_id_map, source_genome_file_map, len(genome_ids), batch_dir)

    start = time.time()
    # RUN checkM2 predict
    command = ['checkm2', 'predict',
               '--output-directory', batch_dir,
               '--threads', str(program_threads),
               '--force',  # will overwrite output directory contents
               '--input'] + fna_files

    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    _run_command(command, debug=debug, log_dir=os.path.join(batch_dir, 'checkm2_log'))
    end_time = time.time()
    print(
        f'Used {round((end_time - start) / 60, 2)} minutes to execute checkM2 predict for {len(genome_ids)} genomes')

    # TODO: inspect stdout for failed ids or do it in the parser program
    return failed_ids


def main():
    parser = argparse.ArgumentParser(
        description='PROTOTYPE - Compute genome attributes in addition to GTDB metadata.')
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional arguments')

    # Required flag arguments
    required.add_argument('--tools', required=True, type=str, nargs='+',
                          help='Tools to be executed. (e.g. gtdb_tk, checkm2, etc.)')
    required.add_argument(f'--{loader_common_names.LOAD_VER_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.LOAD_VER_DESCR)
    required.add_argument('--source_data_dir', required=True, type=str,
                          help='Source data (genome files) directory. '
                               '(e.g. /global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207')

    # Optional arguments
    optional.add_argument(f'--{loader_common_names.KBASE_COLLECTION_ARG_NAME}', type=str,
                          default=loader_common_names.DEFAULT_KBASE_COLL_NAME,
                          help=loader_common_names.KBASE_COLLECTION_DESCR)
    optional.add_argument('--root_dir', type=str, default=loader_common_names.ROOT_DIR,
                          help='Root directory.')
    optional.add_argument('--threads', type=int,
                          help='Total number of threads used by the script. (default: half of system cpu count)')
    optional.add_argument('--program_threads', type=int, default=32,
                          help='Number of threads to execute a single tool command. '
                               'threads / program_threads determines the number of batches. (default: 32)')
    optional.add_argument('--node_id', type=str, default=str(uuid.uuid4()),
                          help='node ID for running job')
    optional.add_argument('--debug', action='store_true',
                          help='Debug mode.')
    optional.add_argument('--genome_id_file', type=argparse.FileType('r'),
                          help="tab separated file containing genome ids for the running job "
                               "(requires 'genome_id' as the column name)")
    optional.add_argument('--source_file_ext', type=str, default='.fa',
                          help='Select files from source data directory that match the given extension.')
    args = parser.parse_args()

    load_ver = getattr(args, loader_common_names.LOAD_VER_ARG_NAME)
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    tools = args.tools
    source_data_dir = args.source_data_dir
    root_dir = args.root_dir
    threads = args.threads
    program_threads = args.program_threads
    debug = args.debug
    genome_id_file = args.genome_id_file
    node_id = args.node_id
    source_file_ext = args.source_file_ext

    # get all genome ids (folder name) from source data directory
    all_genome_ids = [path for path in os.listdir(source_data_dir) if
                      os.path.isdir(os.path.join(source_data_dir, path))]

    if not all_genome_ids:
        raise ValueError('Please download genome files first.')

    # get genome ids for the running job
    if genome_id_file:
        # parse genome ids from tab separated file
        with genome_id_file:
            df = pd.read_csv(genome_id_file, sep='\t')
            try:
                genome_ids = df['genome_id']
            except KeyError:
                raise ValueError('Please ensure genome_id column exists in the genome id file.')

        if not set(all_genome_ids) >= set(genome_ids):
            raise ValueError(f'Genome files for {set(genome_ids) - set(all_genome_ids)} are not downloaded yet')
    else:
        # executing all genomes
        genome_ids = all_genome_ids

    total_count = len(genome_ids)

    if not threads:
        threads = max(int(multiprocessing.cpu_count() * min(SYSTEM_UTILIZATION, 1)), 1)
    threads = max(1, threads)

    for tool in tools:
        # TODO: creating an interface that defines all the tool-specific methods
        # execute each tooling in series
        try:
            comp_ops = getattr(sys.modules[__name__], tool)
        except AttributeError as e:
            raise ValueError(
                f'Please implement method for: [{tool}]\n'
                f'NOTE: Method name should be exactly the same as the tool name') from e

        # place computed results to COLLECTION_DATA_DIR directory
        work_dir = os.path.join(root_dir, loader_common_names.COLLECTION_DATA_DIR, kbase_collection,
                                load_ver, tool)

        start = time.time()
        print(f"Start executing {tool} with {threads} threads")
        if tool in SERIES_TOOLS:
            failed_ids = comp_ops(genome_ids, work_dir, source_data_dir, debug, threads)
        else:
            # call tool execution in parallel
            num_batches = max(math.floor(threads / program_threads), 1)
            chunk_size = math.ceil(len(genome_ids) / num_batches)  # distribute genome ids evenly across batches
            batch_input = [(genome_ids[i: i + chunk_size],
                            work_dir,
                            source_data_dir,
                            debug,
                            program_threads,
                            batch_number,
                            node_id) for batch_number, i in enumerate(range(0, total_count, chunk_size))]
            pool = multiprocessing.Pool(processes=num_batches)
            batch_result = pool.starmap(comp_ops, batch_input)
            failed_ids = list(itertools.chain.from_iterable(batch_result))
        print(
            f'In total used {round((time.time() - start) / 60, 2)} minutes to execute {tool} for {total_count} genomes')

        if failed_ids:
            print(f'Failed to execute {tool} for {failed_ids}')
        else:
            print(f'Successfully executed all genomes for {tool}')


if __name__ == "__main__":
    main()
