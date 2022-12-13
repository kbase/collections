"""
PROTOTYPE

Before calling this script, the required input files for each tooling should be already downloaded at
[root_dir]/sourcedata/[source]/[release_ver] using the `ncbi_downloader.py` script.

This script will compute and save result files to [root_dir/collectionsdata/[kbase_collection]/[load_ver]/[tool_name].


usage: compute_genome_attribs.py [-h] --tools TOOLS [TOOLS ...] --load_ver LOAD_VER --source_data_dir
                                 SOURCE_DATA_DIR [--kbase_collection KBASE_COLLECTION]
                                 [--root_dir ROOT_DIR] [--threads THREADS]
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
  --threads THREADS     Total number of threads. (default: half of system cpu count)
  --program_threads PROGRAM_THREADS
                        Number of threads to execute tool command. (default: 32)
  --node_id NODE_ID     node ID for running job
  --debug               Debug mode.
  --genome_id_file GENOME_ID_FILE
                        tab separated file containing genome ids for the running job (requires
                        'genome_id' as the column name)


e.g. python compute_genome_attribs.py --tools gtdb_tk checkm2 --load_ver r207.kbase.1 --source_data_dir /global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207 --debug

NOTE:
NERSC file structure for GTDB:
/global/cfs/cdirs/kbase/collections/collectionsdata/ -> [kbase_collection] -> [load_ver] -> [tool_name]
e.g.
/global/cfs/cdirs/kbase/collections/collectionsdata/GTDB -> r207.kbase.1 -> gtdb_tk -> batch_0_size_x_node_x -> result files
                                                                                    -> batch_1_size_x_node_x -> result files
                                                                         -> checkm2 -> batch_0_size_x_node_x -> result files
                                                                                    -> batch_1_size_x_node_x -> result files

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

from src.loaders.common.nersc_file_structure import ROOT_DIR, COLLECTION_DATA_DIR

SERIES_TOOLS = []  # Tools cannot be executed in parallel

# Fraction amount of system cores can be utilized
# (i.e. 0.5 - program will use 50% of total processors,
#       0.1 - program will use 10% of total processors)
SYSTEM_UTILIZATION = 0.5


def _find_genome_file(genome_id, file_ext, source_data_dir, exclude_file_name_substr=None):
    genome_path = os.path.join(source_data_dir, genome_id)

    if not os.path.exists(genome_path):
        raise ValueError(f'Cannot find file directory for: {genome_id}')

    genome_files = [os.path.join(genome_path, f) for f in os.listdir(genome_path) if f.endswith(file_ext)]

    if exclude_file_name_substr:
        genome_files = [f for f in genome_files if
                        all(name_substr not in f for name_substr in exclude_file_name_substr)]

    return genome_files


def _run_command(command, debug=False, log_dir=''):
    # execute command.
    # if debug is set, write output of stdout and stderr to files (named as stdout and stderr) to log_dir

    if debug:
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


def gtdb_tk(genome_ids, work_dir, source_data_dir, debug, program_threads, batch_number,
            node_id, run_steps=False):
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

    with open(batch_file_path, "w") as batch_file:
        for genome_id in genome_ids:
            genome_file = _find_genome_file(genome_id, 'genomic.fna.gz', source_data_dir,
                                            exclude_file_name_substr=['cds_from', 'rna_from', 'ERR'])

            if genome_file and len(genome_file) == 1:
                batch_file.write(f'{genome_file[0]}\t{os.path.basename(genome_file[0])}\n')
            else:
                print(f'Cannot retrieve target file. Please check download folder for genome: {genome_id}')

    if run_steps:
        _run_gtdb_tk_steps(batch_file_path, batch_dir, debug, genome_ids, program_threads)
    else:
        _run_gtdb_tk_classify_wf(batch_file_path, batch_dir, debug, genome_ids, program_threads)

    # TODO: inspect stdout for failed ids or do it in the parser program
    return failed_ids


def checkm2(genome_ids, work_dir, source_data_dir, debug, program_threads, batch_number, node_id):
    # NOTE: require Python <= 3.9
    # Many checkm2 dependencies (e.g. scikit-learn=0.23.2, tensorflow, diamond, etc.) support Python version up to 3.9

    failed_ids, size = list(), len(genome_ids)
    print(f'Start executing checkM2 for {len(genome_ids)} genomes')

    batch_dir = _create_batch_dir(work_dir, batch_number, size, node_id)

    # retrieve genomic.fna.gz files
    fna_files = list()
    for genome_id in genome_ids:
        genome_file = _find_genome_file(genome_id, 'genomic.fna.gz', source_data_dir,
                                        exclude_file_name_substr=['cds_from', 'rna_from', 'ERR'])

        if genome_file and len(genome_file) == 1:
            fna_files.append(str(genome_file[0]))
        else:
            print(f'Cannot retrieve target file. Please check download folder for genome: {genome_id}')

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
    required.add_argument('--load_ver', required=True, type=str,
                          help='KBase load version. (e.g. r207.kbase.1)')
    required.add_argument('--source_data_dir', required=True, type=str,
                          help='Source data (genome files) directory. '
                               '(e.g. /global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207')

    # Optional arguments
    optional.add_argument('--kbase_collection', type=str, default='GTDB',
                          help='KBase collection identifier name. (default: GTDB)')
    optional.add_argument('--root_dir', type=str, default=ROOT_DIR,
                          help='Root directory.')
    optional.add_argument('--threads', type=int,
                          help='Total number of threads. (default: half of system cpu count)')
    optional.add_argument('--program_threads', type=int, default=32,
                          help='Number of threads to execute tool command. (default: 32)')
    optional.add_argument('--node_id', type=str, default=str(uuid.uuid4()),
                          help='node ID for running job')
    optional.add_argument('--debug', action='store_true',
                          help='Debug mode.')
    optional.add_argument('--genome_id_file', type=argparse.FileType('r'),
                          help="tab separated file containing genome ids for the running job (requires 'genome_id' as the column name)")
    args = parser.parse_args()

    (tools,
     load_ver,
     source_data_dir,
     kbase_collection,
     root_dir,
     threads,
     program_threads,
     debug,
     genome_id_file,
     node_id) = (args.tools, args.load_ver, args.source_data_dir,
                 args.kbase_collection, args.root_dir, args.threads, args.program_threads,
                 args.debug, args.genome_id_file, args.node_id)

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
        # execute each tooling in series
        try:
            comp_ops = getattr(sys.modules[__name__], tool)
        except AttributeError as e:
            raise ValueError(
                f'Please implement method for: [{tool}]\n'
                f'NOTE: Method name should be exactly the same as the tool name') from e

        # place computed results to COLLECTION_DATA_DIR directory
        work_dir = os.path.join(root_dir, COLLECTION_DATA_DIR, kbase_collection,
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
            pool = multiprocessing.Pool(processes=threads)
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
