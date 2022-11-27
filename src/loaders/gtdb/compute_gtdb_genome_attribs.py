"""
PROTOTYPE

Before calling this script, the required input files for each tooling should be already downloaded at
[root_dir]/[kbase_collection]/[load_ver] using the `ncbi_downloader.py` script.

This script will compute and save result files to [root_dir/../computed_genome_attributes]/[kbase_collection]/[load_ver].

For each tooling, there should be a corresponding method with the same name as the tool that executes the tool.

usage: compute_gtdb_genome_attribs.py [-h] --tools TOOLS [TOOLS ...] --load_ver LOAD_VER [--kbase_collection KBASE_COLLECTION] [--root_dir ROOT_DIR] [--cpus CPUS]
                                      [--program_threads PROGRAM_THREADS] [--chuck_size CHUCK_SIZE] [--debug]

optional arguments:
  -h, --help            show this help message and exit

required named arguments:
  --tools TOOLS [TOOLS ...]
                        Tools to be executed. (e.g. gtdb_tk, checkm2, etc.)
  --load_ver LOAD_VER   KBase load version. (e.g. r207.kbase.1)

optional arguments:
  --kbase_collection KBASE_COLLECTION
                        KBase collection identifier name. (default: GTDB)
  --root_dir ROOT_DIR   Root directory. (default: /global/cfs/cdirs/kbase/collections/genome_attributes)
  --cpus CPUS           Total number of cups. (default: half of system cpu count)
  --program_threads PROGRAM_THREADS
                        Number of threads for each program execution.
  --chuck_size CHUCK_SIZE
                        Number of work items attributed to each CPU. (default: evenly distribute items to all CPUs)
  --debug               Debug mode.

e.g. compute_gtdb_genome_attribs.py --tools gtdb_tk --load_ver r207.kbase.1 --debug
"""
import argparse
import itertools
import multiprocessing
import os
import subprocess
import sys
from pathlib import Path
import time

SERIES_TOOLS = []  # Tools cannot be executed in parallel

# Fraction amount of system cores can be utilized
# (i.e. 0.5 - program will use 50% of total processors,
#       0.1 - program will use 10% of total processors)
SYSTEM_UTILIZATION = 0.5


def _find_genome_file(genome_id, file_ext, root_dir, kbase_collection, load_ver):
    genome_path = os.path.join(root_dir, kbase_collection, load_ver, genome_id)

    if not os.path.exists(genome_path):
        raise ValueError(f'Cannot find file directory for: {genome_id}')

    genome_files = [os.path.join(genome_path, f) for f in os.listdir(genome_path) if f.endswith(file_ext)]

    return genome_files


def _run_command(command, debug=False, log_dir=''):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = p.communicate()

    if debug:
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(log_dir, 'stdout'), "w") as std_out:
            std_out.write(str(stdout))
        with open(os.path.join(log_dir, 'stderr'), "w") as std_err:
            std_err.write(str(stderr))

    return stdout, stderr


def _create_batch_dir(work_dir, batch_number, size):
    # create working directory for each batch
    batch_dir = os.path.join(work_dir, f'batch_{batch_number}_size_{size}')
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
        f'Used {round((end_identify_time - start) / 60, 2)} minutes to execute gtdbtk identify for {len(genome_ids)} genomes')

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
        f'Used {round((end_align_time - end_identify_time) / 60, 2)} minutes to execute gtdbtk align for {len(genome_ids)} genomes')

    # RUN gtdbtk classify step
    command = ['gtdbtk', 'classify',
               '--batchfile', batch_file_path,
               '--align_dir', os.path.join(work_dir, 'align'),
               '--out_dir', os.path.join(work_dir, 'classify'),
               '--cpus', str(program_threads)]
    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    _run_command(command, debug=debug, log_dir=os.path.join(work_dir, 'align_log'))
    end_align_time = time.time()
    print(
        f'Used {round((end_align_time - end_identify_time) / 60, 2)} minutes to execute gtdbtk classify for {len(genome_ids)} genomes')


def _run_gtdb_tk_classify_wf(batch_file_path, work_dir, debug, genome_ids, program_threads):
    # run GTDB-TK classify_wf workflow
    start = time.time()

    # RUN gtdbtk classify_wf
    command = ['gtdbtk', 'classify_wf',
               '--batchfile', batch_file_path,
               '--out_dir', work_dir,
               '--cpus', str(program_threads)]
    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    _run_command(command, debug=debug, log_dir=os.path.join(work_dir, 'identify_log'))
    end_time = time.time()
    print(
        f'Used {round((end_time - start) / 60, 2)} minutes to execute gtdbtk classify_wf for {len(genome_ids)} genomes')


def gtdb_tk(genome_ids, work_dir, root_dir, kbase_collection, load_ver, debug, program_threads, batch_number,
            run_steps=True):
    # NOTE: Require defining the 'GTDBTK_DATA_PATH' environment variable
    #       e.g. export GTDBTK_DATA_PATH=/global/cfs/cdirs/kbase/collections/libraries/gtdb_tk/release207_v2
    #       https://ecogenomics.github.io/GTDBTk/installing/index.html#gtdb-tk-reference-data
    #
    #       Ensure that Third-party software are on the system path.
    #       https://ecogenomics.github.io/GTDBTk/installing/index.html#installing-third-party-software

    failed_ids, size = list(), len(genome_ids)
    print(f'Start executing GTDB-TK for {size} genomes')

    batch_dir = _create_batch_dir(work_dir, batch_number, size)

    # create the batch file
    # tab separated in 2 columns (FASTA file, genome ID)
    batch_file_name = f'genome.fasta.list'
    batch_file_path = os.path.join(batch_dir, batch_file_name)

    with open(batch_file_path, "w") as batch_file:
        for genome_id in genome_ids:
            genome_files = _find_genome_file(genome_id, 'genomic.fna.gz', root_dir, kbase_collection, load_ver)
            # Only need xx.genomic.fna.gz file.
            # Excluding potential xx.cds_from_genomic.fna.gz and xx.rna_from_genomic.fna.gz files
            genome_file = [f for f in genome_files if 'cds_from' not in f and 'rna_from' not in f][0]
            batch_file.write(f'{genome_file}\t{os.path.basename(genome_file)}\n')

    if run_steps:
        _run_gtdb_tk_steps(batch_file_path, batch_dir, debug, genome_ids, program_threads)
    else:
        _run_gtdb_tk_classify_wf(batch_file_path, batch_dir, debug, genome_ids, program_threads)

    # TODO: inspect each stdout for failed ids
    return failed_ids


def checkm2(genome_ids, work_dir, root_dir, kbase_collection, load_ver, debug, program_threads, batch_number):
    # NOTE: require Python <= 3.9
    # Many checkm2 dependencies (e.g. scikit-learn=0.23.2, tensorflow, diamond, etc.) support Python version up to 3.9

    failed_ids, size = list(), len(genome_ids)
    print(f'Start executing checkM2 for {len(genome_ids)} genomes')

    batch_dir = _create_batch_dir(work_dir, batch_number, size)

    # retrieve genomic.fna.gz files
    fna_files = list()
    for genome_id in genome_ids:
        genome_files = _find_genome_file(genome_id, 'genomic.fna.gz', root_dir, kbase_collection, load_ver)
        # Only need xx.genomic.fna.gz file.
        # Excluding potential xx.cds_from_genomic.fna.gz and xx.rna_from_genomic.fna.gz files
        genome_file = [f for f in genome_files if 'cds_from' not in f and 'rna_from' not in f][0]
        fna_files.append(genome_file)

    start = time.time()

    # RUN checkM2 predict
    command = ['checkm2', 'predict',
               '--output-directory', batch_dir,
               '--threads', str(program_threads),
               '--force',
               '--input']
    command.extend(fna_files)
    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    _run_command(command, debug=debug, log_dir=os.path.join(batch_dir, 'checkm2_log'))
    end_time = time.time()
    print(
        f'Used {round((end_time - start) / 60, 2)} minutes to execute checkM2 predict for {len(genome_ids)} genomes')

    # TODO: inspect stdout for failed ids
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

    # Optional arguments
    optional.add_argument('--kbase_collection', type=str, default='GTDB',
                          help='KBase collection identifier name. (default: GTDB)')
    optional.add_argument('--root_dir', type=str, default='/global/cfs/cdirs/kbase/collections/genome_attributes',
                          help='Root directory. (default: /global/cfs/cdirs/kbase/collections/genome_attributes)')
    optional.add_argument('--cpus', type=int,
                          help='Total number of cups. (default: half of system cpu count)')
    optional.add_argument('--program_threads', type=int,
                          help='Number of threads for each program execution.')
    optional.add_argument('--chuck_size', type=int,
                          help='Number of work items attributed to each CPU. (default: evenly distribute items to all CPUs)')
    optional.add_argument('--debug', action='store_true',
                          help='Debug mode.')
    args = parser.parse_args()

    (tools,
     load_ver,
     kbase_collection,
     root_dir,
     cpus,
     chuck_size,
     debug,
     program_threads) = (args.tools, args.load_ver, args.kbase_collection, args.root_dir, args.cpus,
                         args.chuck_size, args.debug, args.program_threads)

    # get genome ids
    genome_file_dir = os.path.join(root_dir, kbase_collection, load_ver)
    genome_ids = [path for path in os.listdir(genome_file_dir) if os.path.isdir(os.path.join(genome_file_dir, path))]
    total_count = len(genome_ids)

    if not genome_ids:
        raise ValueError('Please download genome files first.')

    if not cpus:
        cpus = max(int(multiprocessing.cpu_count() * min(SYSTEM_UTILIZATION, 1)), 1)
    print(f'Utilizing {cpus} CPUs')

    for tool in tools:
        # execute each tooling in series
        try:
            comp_ops = getattr(sys.modules[__name__], tool)
        except AttributeError as e:
            raise ValueError(
                f'Please implement method for: [{tool}]\nNOTE: Method name should be exactly the same as the tool name') from e

        # place computed results to computed_genome_attributes directory
        if Path(root_dir).resolve().name == 'genome_attributes':
            work_dir = os.path.join(Path(root_dir).resolve().parents[0], 'computed_genome_attributes', kbase_collection,
                                    load_ver, tool)
        else:
            work_dir = os.path.join(root_dir, 'computed_genome_attributes', kbase_collection,
                                    load_ver, tool)

        start = time.time()
        if tool in SERIES_TOOLS:
            failed_ids = comp_ops(genome_ids, work_dir, root_dir, kbase_collection, load_ver, debug, cpus)
        else:
            # call tool execution in parallel
            if not chuck_size:
                chuck_size = max(total_count // (cpus - 1), 1)

            if not program_threads:
                processes = min(max(total_count // chuck_size, 1), cpus)  # total number of batches
                program_threads = max(cpus // processes,
                                      1)  # threads for each execution (maximum utilization of all cpus)
            else:
                processes = max(cpus // program_threads, 1)

            batch_input = [(genome_ids[i: i + chuck_size],
                            work_dir,
                            root_dir,
                            kbase_collection,
                            load_ver,
                            debug,
                            program_threads,
                            batch_number) for batch_number, i in enumerate(range(0, total_count, chuck_size))]
            pool = multiprocessing.Pool(processes=processes)
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
