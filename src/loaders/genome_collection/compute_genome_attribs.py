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
                        KBase collection identifier name. (default: loader_common_names.DEFAULT_KBASE_COLL_NAME)
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
import math
import multiprocessing
import os
import subprocess
import sys
import time
import uuid

import pandas as pd

from src.loaders.common import loader_common_names

SERIES_TOOLS = ['microtrait']  # Tools cannot be executed in parallel

# Fraction amount of system cores can be utilized
# (i.e. 0.5 - program will use 50% of total processors,
#       0.1 - program will use 10% of total processors)
SYSTEM_UTILIZATION = 0.5

# source genome files can be missing for those collections
# genomes with missing files will be skipped rather than raising an error
# TODO having download script not create empty directories for genomes with missing files so that we no longer need this
IGNORE_MISSING_GENOME_FILES_COLLECTIONS = ['GTDB']


def _find_genome_file(genome_id, file_ext, source_data_dir, collection, exclude_file_name_substr=None,
                      expected_file_count=1):
    # Finds genome file(s) for a given genome ID in a source data directory,
    # excluding files whose name contains specified substrings.

    # Return a list of matching genome file paths or an empty list if no matching files are found and the collection is
    # in the IGNORE_MISSING_GENOME_FILES_COLLECTIONS list.

    # In most cases, our focus is solely on the {genome_id}.genomic.fna.gz file from NCBI, while excluding
    # {genome_id}.cds_from_genomic.fna.gz, {genome_id}.rna_from_genomic.fna.gz, and
    # {genome_id}.ERR_genomic.fna.gz files.
    if exclude_file_name_substr is None:
        exclude_file_name_substr = ['cds_from', 'rna_from', 'ERR']
    genome_path = os.path.join(source_data_dir, genome_id)

    if not os.path.exists(genome_path):
        raise ValueError(f'Cannot find file directory for: {genome_id}')

    genome_files = [os.path.join(genome_path, f) for f in os.listdir(genome_path) if f.endswith(file_ext)]

    if exclude_file_name_substr:
        genome_files = [f for f in genome_files if
                        all(name_substr not in f for name_substr in exclude_file_name_substr)]

    # Raise an error if no genome files are found and the collection is not in the ignored collections
    if not genome_files and collection not in IGNORE_MISSING_GENOME_FILES_COLLECTIONS:
        raise ValueError(f'Cannot find target file(s) for: {genome_id}')

    # Raise an error if the number of files found does not match the expected file count
    if genome_files and len(genome_files) != expected_file_count:
        raise ValueError(f'Found {len(genome_files)} files for {genome_id} but expected {expected_file_count}')

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


def _retrieve_tool_genome_identifier(tool_name, original_genome_id, genome_file_name=None, source_file_ext=None):
    """
    Generate the unique genome identifier linked to the tool, which corresponds to the original genome ID.

    The method is specific to the tool being used, as each tool has its own logic for generating unique genome IDs.
    """

    if tool_name == 'checkm2':
        # CheckM2 uses the base name (without extension) of genome_file as the genome identifier
        base_name = genome_file_name.split(source_file_ext)[0]
        return base_name

    elif tool_name in ['gtdb_tk', 'microtrait']:
        # When creating the batch file for GTDB-TK, we use the genome ID as the identifier for each genome.
        return original_genome_id
    else:
        raise ValueError(f'the method for tool {tool_name} has not been implemented.')


def _create_genome_metadata_file(genomes_meta, batch_dir):
    # create tab separated metadata file with tool generated genome identifier, original genome id and
    # source genome file info.

    # create tool genome identifier metadata file
    genome_meta_file_path = os.path.join(batch_dir, loader_common_names.GENOME_METADATA_FILE)
    with open(genome_meta_file_path, "w") as meta_file:
        for genome_id, genome_meta_info in genomes_meta.items():
            meta_file.write(f'{genome_meta_info["tool_identifier"]}\t{genome_id}\t{genome_meta_info["source_file"]}\n')


def _retrieve_genome_file(genome_id, source_data_dir, source_file_ext, collection, tool,
                          exclude_file_name_substr=None):
    # retrieve the genome file associated with genome_id
    # return the genome file path if it exists, otherwise raise an error if collection is not in the list of
    # IGNORE_MISSING_GENOME_FILES_COLLECTIONS

    genome_files = _find_genome_file(genome_id, source_file_ext, source_data_dir, collection,
                                     exclude_file_name_substr=exclude_file_name_substr)

    tool_identifier, genome_file = None, None
    if genome_files:
        genome_file = genome_files[0]  # only one genome file is expected
        tool_identifier = _retrieve_tool_genome_identifier(
            tool, genome_id, genome_file_name=os.path.basename(genome_file), source_file_ext=source_file_ext)

    return genome_file, tool_identifier


def _prepare_tool(tool_name, work_dir, batch_number, size, node_id, genome_ids, source_data_dir, source_file_ext,
                  kbase_collection):
    # Prepares for tool execution by creating a batch directory and retrieving genome files with associated metadata.

    batch_dir = _create_batch_dir(work_dir, batch_number, size, node_id)

    # Retrieve genome files and associated metadata for each genome ID
    genome_meta = dict()
    for genome_id in genome_ids:
        genome_file, tool_identifier = _retrieve_genome_file(
            genome_id, source_data_dir, source_file_ext, kbase_collection, tool_name)
        if genome_file:
            genome_meta[genome_id] = {'tool_identifier': tool_identifier, 'source_file': genome_file}

    return batch_dir, genome_meta


def _run_microtrait(genome_id, fna_file, debug):
    microtrait_result_dict = {'genome_id': genome_id, 'source_file': fna_file}

    # Load the microtrait package
    # importr("microtrait")
    # TODO: implement the Rscript to run microtrait, potentially an isolated Rscript and get rid of the rpy2 dependency

    return microtrait_result_dict


def gtdb_tk(genome_ids, work_dir, source_data_dir, debug, program_threads, batch_number,
            node_id, source_file_ext, kbase_collection, run_steps=False):
    # NOTE: Require defining the 'GTDBTK_DATA_PATH' environment variable
    #       e.g. export GTDBTK_DATA_PATH=/global/cfs/cdirs/kbase/collections/libraries/gtdb_tk/release207_v2
    #       https://ecogenomics.github.io/GTDBTk/installing/index.html#gtdb-tk-reference-data
    #
    #       Ensure that Third-party software are on the system path.
    #       https://ecogenomics.github.io/GTDBTk/installing/index.html#installing-third-party-software

    size = list()
    print(f'Start executing GTDB-TK for {size} genomes')

    batch_dir, genomes_meta = _prepare_tool('gtdb_tk', work_dir, batch_number, size, node_id, genome_ids,
                                            source_data_dir, source_file_ext, kbase_collection)

    # create the batch file
    # tab separated in 2 columns (FASTA file, genome ID)
    batch_file_name = f'genome.fasta.list'
    batch_file_path = os.path.join(batch_dir, batch_file_name)
    with open(batch_file_path, "w") as batch_file:
        for genome_id, genome_meta in genomes_meta.items():
            batch_file.write(f'{genome_meta["source_file"]}\t{genome_id}\n')

    if run_steps:
        _run_gtdb_tk_steps(batch_file_path, batch_dir, debug, genome_ids, program_threads)
    else:
        _run_gtdb_tk_classify_wf(batch_file_path, batch_dir, debug, genome_ids, program_threads)

    _create_genome_metadata_file(genomes_meta, batch_dir)


def checkm2(genome_ids, work_dir, source_data_dir, debug, program_threads, batch_number, node_id, source_file_ext,
            kbase_collection):
    # NOTE: require Python <= 3.9
    # Many checkm2 dependencies (e.g. scikit-learn=0.23.2, tensorflow, diamond, etc.) support Python version up to 3.9

    size = list()
    print(f'Start executing checkM2 for {size} genomes')

    batch_dir, genomes_meta = _prepare_tool('checkm2', work_dir, batch_number, size, node_id, genome_ids,
                                            source_data_dir, source_file_ext, kbase_collection)

    fna_files = [str(genome_meta['source_file']) for genome_meta in genomes_meta.values()]

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
        f'Used {round((end_time - start) / 60, 2)} minutes to execute checkM2 predict for {size} genomes')

    _create_genome_metadata_file(genomes_meta, batch_dir)


def microtrait(genome_ids, work_dir, source_data_dir, debug, program_threads, node_id, source_file_ext,
               kbase_collection):
    size = len(genome_ids)
    print(f'Start executing MicroTrait for {size} genomes')

    batch_dir, genomes_meta = _prepare_tool('microtrait', work_dir, 'series', size, node_id, genome_ids,
                                            source_data_dir, source_file_ext, kbase_collection)
    start = time.time()

    # RUN MicroTrait in parallel with multiprocessing
    args_list = [(genome_id, genome_meta['source_file'], debug) for genome_id, genome_meta in genomes_meta.items()]
    pool = multiprocessing.Pool(processes=program_threads)
    results = pool.starmap(_run_microtrait, args_list)
    pool.close()
    pool.join()

    # TODO process results from _run_microtrait

    end_time = time.time()
    print(
        f'Used {round((end_time - start) / 60, 2)} minutes to execute MicroTrait for {size} genomes')

    _create_genome_metadata_file(genomes_meta, batch_dir)


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
            comp_ops(genome_ids, work_dir, source_data_dir, debug, threads, node_id, source_file_ext, kbase_collection)
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
                            node_id,
                            source_file_ext,
                            kbase_collection) for batch_number, i in enumerate(range(0, total_count, chunk_size))]
            pool = multiprocessing.Pool(processes=num_batches)
            pool.starmap(comp_ops, batch_input)
            pool.close()
            pool.join()
        print(
            f'In total used {round((time.time() - start) / 60, 2)} minutes to execute {tool} for {total_count} genomes')


if __name__ == "__main__":
    main()
