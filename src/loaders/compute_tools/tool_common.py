"""
Holds common code for processing input data via some tool and getting results in the context
of the KBase collections service.
"""

import argparse
import datetime
import gzip
import math
import multiprocessing
import os
import shutil
import subprocess
import sys
import time
import uuid

import pandas as pd
from pathlib import Path

from src.loaders.common import loader_common_names


DATA_ID_COLUMN_HEADER = "genome_id"  # TODO DATA_ID change to data ID for generality

# TODO CODE move these to loader common names to share with parser
META_SOURCE_FILE = "source_file"
META_TOOL_IDENTIFIER = "tool_identifier"

# Fraction amount of system cores can be utilized
# (i.e. 0.5 - program will use 50% of total processors,
#       0.1 - program will use 10% of total processors)
SYSTEM_UTILIZATION = 0.5  # This might need to be customizable per tool

# source genome files can be missing for those collections
# genomes with missing files will be skipped rather than raising an error
# TODO having download script not create empty directories for genomes with missing files
#      so that we no longer need this
IGNORE_MISSING_GENOME_FILES_COLLECTIONS = ['GTDB']
# TODO DOWNLOAD if we settle on a standard file name schem for downloaders we can get
#               rid of this
STANDARD_FILE_EXCLUDE_SUBSTRINGS = ['cds_from', 'rna_from', 'ERR']


class ToolRunner:

    def __init__(self, tool_name: str, tool_data_id_from_filename=False):
        """
        Create the runner. Expects arguments on the command line as:
        TODO

        tool_name - the name of the tool that will be run. Used as a unique identifier.
        tool_data_id_from_filename - True if the tool uses the filename without extension
            as the data ID, meaning the tool runner needs to map from the data ID to the filename
        """
        # TODO NOW document command line args
        self._tool = tool_name
        # TODO DOWNLOAD if we settle on a standard file name schem for downloaders we can get
        #               rid of this
        self._tool_data_id_from_filename
        args = self._parse_args()
        self._load_ver = getattr(args, loader_common_names.LOAD_VER_ARG_NAME)
        self._kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
        self._source_data_dir = Path(args.source_data_dir)
        self._root_dir = Path(args.root_dir)
        self._threads = args.threads
        self._program_threads = args.program_threads
        self._debug = args.debug
        self._data_id_file = args.data_id_file
        self._node_id = args.node_id
        self._source_file_ext = args.source_file_ext
        self._data_ids = self._get_data_ids()

        if not self._threads:
            threads = max(int(multiprocessing.cpu_count() * min(SYSTEM_UTILIZATION, 1)), 1)
        self._threads = max(1, threads)

        self._work_dir = Path(
            self._root_dir,
            loader_common_names.COLLECTION_DATA_DIR,
            self._kbase_collection,
            self._load_ver,
            self._tool
        )

    def _parse_args(self):
        parser = argparse.ArgumentParser(
            description='PROTOTYPE - Compute genome attributes in addition to GTDB metadata.')
        required = parser.add_argument_group('required named arguments')
        optional = parser.add_argument_group('optional arguments')

        # Required flag arguments
        required.add_argument(
            f'--{loader_common_names.LOAD_VER_ARG_NAME}', required=True,type=str,
            help=loader_common_names.LOAD_VER_DESCR
        )
        required.add_argument(
            '--source_data_dir', required=True, type=str,
            help='Source data (genome files) directory. '
                + '(e.g. /global/cfs/cdirs/kbase/collections/sourcedata/GTDB/r207'
        )

        # Optional arguments
        optional.add_argument(
            f'--{loader_common_names.KBASE_COLLECTION_ARG_NAME}', type=str,
            default=loader_common_names.DEFAULT_KBASE_COLL_NAME,
            help=loader_common_names.KBASE_COLLECTION_DESCR
        )
        optional.add_argument(
            '--root_dir', type=str, default=loader_common_names.ROOT_DIR, help='Root directory.'
        )
        optional.add_argument(
            '--threads', type=int,
            help='Total number of threads used by the script. (default: half of system cpu count)'
        )
        optional.add_argument(
            '--program_threads', type=int, default=32,
            help='Number of threads to execute a single tool command. '
                + 'threads / program_threads determines the number of batches. (default: 32)'
        )
        optional.add_argument(
            '--node_id', type=str, default=str(uuid.uuid4()), help='node ID for running job'
        )
        optional.add_argument('--debug', action='store_true', help='Debug mode.')
        optional.add_argument(
            '--data_id_file', type=argparse.FileType('r'),
            help="tab separated file containing data ids for the running job "
                + f"(requires '{DATA_ID_COLUMN_HEADER}' as the column name)"
        )
        optional.add_argument(
            '--source_file_ext', type=str, default='.fa',
            help='Select files from source data directory that match the given extension.'
        )
        return parser.parse_args()

    def _get_data_ids(self):
        # get all data IDs (folder name) from source data directory
        all_data_ids = [path for path in os.listdir(self._source_data_dir) if
                        os.path.isdir(os.path.join(self._source_data_dir, path))]

        if not all_data_ids:
            raise ValueError('Please download genome files first.')

        # get data ids for the running job
        if self._data_id_file:
            # parse data ids from tab separated file
            with self._data_id_file:
                df = pd.read_csv(self._data_id_file, sep='\t')
                try:
                    data_ids = df[DATA_ID_COLUMN_HEADER]
                except KeyError:
                    raise ValueError(
                        f"Please ensure {DATA_ID_COLUMN_HEADER} column exists in the "
                        + "data id file.")

            if not set(all_data_ids) >= set(data_ids):
                raise ValueError(
                    f'Data files for {set(data_ids) - set(all_data_ids)} are not downloaded yet')
        else:
            # executing all data IDs
            data_ids = all_data_ids
        return data_ids

    def run_single(self, tool_callable: Callable[[Path, Path, bool], None]):
        """
        Run a tool data file by data file, storing the results in a single batch directory with
        the individual runs stored in directories by the data ID.

        tool_callable - the callable for the tool that takes three arguments: the input file,
            the output directory, and a debug boolean.
        """
        start = time.time()
        batch_dir, genomes_meta = _prepare_tool(
            self._tool,
            self._work_dir,
            "no_batch",
            self._node_id,
            self._data_ids,
            self._source_data_dir,
            self._source_file_ext,
            self._kbase_collection,
            self._tool_data_id_from_filename,
        )

        # RUN tool in parallel with multiprocessing
        args_list = []
        for data_id, meta in genomes_meta.items():
            output_dir = batch_dir / data_id
            os.makedirs(output_dir, exist_ok=True)
            args_list.append((genome_meta[META_SOURCE_FILE], output_dir, self._debug))
        self._execute(self._program_threads, tool_callable, args_list, start, False)
        _create_genome_metadata_file(genomes_meta, batch_dir)
    
    def run_batched(self, tool_callable: Callable[[dict[str, Path], Path, int, bool], None]):
        """
        Run a tool in batched mode, where > 1 data file is processed by the tool in one
        call. Each batch gets its own batch directory.

        tool_callable - the callable for the tool that takes 4 arguments:
            * A dictionary of the data_id to the source file path
            * The output directory for results
            * The number of threads to use for the batch
            * A debug boolean
        """
        start = time.time()
        num_batches = max(math.floor(self._threads / self._program_threads), 1)
        # distribute genome ids evenly across batches
        chunk_size = math.ceil(len(self._data_ids) / num_batches)
        batch_input = []
        for batch_number, i in enumerate(range(0, len(self._data_ids), chunk_size)):
            data_ids = self._data_ids[i: i + chunk_size]
            batch_dir, meta = _prepare_tool(
                self._tool,
                self._work_dir,
                batch_number,
                self._node_id,
                data_ids,
                self._source_data_dir,
                self._source_file_ext,
                self._kbase_collection,
                self._tool_data_id_from_filename,
            )
            ids_to_files = {d: meta[d][META_SOURCE_FILE] for d in data_ids}
            batch_input.append((ids_to_files, batch_dir, self._program_threads, self._debug))
        self._execute(num_batches, tool_callable, batch_input, start, True)
        _create_genome_metadata_file(meta, batch_dir)

    def _execute(
        self,
        threads: int,
        tool_callable: Callable[[list[Any]], None],
        args: list[tuple[Any]],
        start: datetime.datetime,
        total: bool,
    ):
        pool = multiprocessing.Pool(processes=threads)
        pool.starmap(tool_callable, args)
        pool.close()
        pool.join()
        prefix = "In total used" if total else "Used"
        print(f"{prefix} {round((time.time() - start) / 60, 2)} minutes to "
            + f"execute {self._tool} for {len(self._data_ids)} data units"
        )


def unpack_gz_file(gz_file):
    """
    Unpack a gzipped file. Does nothing if the file without the 'gz' extension already exists.
    """

    output_file_path = os.path.splitext(gz_file)[0]
    if os.path.exists(output_file_path):
        print(f'file {output_file_path} already exists. Skipping unpacking {gz_file}')
        return output_file_path

    print(f'unpacking {gz_file}')
    with gzip.open(gz_file, 'rb') as f_in, open(output_file_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    return output_file_path


def run_command(command: list[str], debug: bool = False, log_dir=''):
    """
    Run a command in a subprocess.

    command - the command to run.
    debug - true to record program logs in `log_dir`
    log_dir - the directory in which to record program logs if `debug` is true.
    """
    if debug:
        os.makedirs(log_dir, exist_ok=True)
        with open(
            os.path.join(log_dir, 'stdout'), "w"
        ) as std_out, open(
            os.path.join(log_dir, 'stderr'), "w"
        ) as std_err:
            p = subprocess.Popen(command, stdout=std_out, stderr=std_err, text=True)
    else:
        p = subprocess.Popen(command)

    exit_code = p.wait()

    if exit_code != 0:
        raise ValueError(f'The command {command} failed with exit code {exit_code}')


def _create_genome_metadata_file(genomes_meta, batch_dir):
    # create tab separated metadata file with tool generated genome identifier,
    # original genome id and source genome file info.

    # create tool genome identifier metadata file
    genome_meta_file_path = os.path.join(batch_dir, loader_common_names.GENOME_METADATA_FILE)
    with open(genome_meta_file_path, "w") as meta_file:
        for genome_id, genome_meta_info in genomes_meta.items():
            meta_file.write(
                f'{genome_meta_info[META_TOOL_IDENTIFIER]}\t{genome_id}\t'
                + f'{genome_meta_info[META_SOURCE_FILE]}\n'
            )


# Might be cleaner to add to the class, but making it strictly procedural is simpler in
# other ways. Meh
def _prepare_tool(
    tool_name: str,
    work_dir: Path,
    batch_id: str,
    node_id: str,
    data_ids: list[str],
    source_data_dir: Path,
    source_file_ext: str,
    kbase_collection: str,
    data_id_from_filename: bool,
) -> tuple[Path, dict[str, dict[str, str | Path]]]:
    # Prepares for tool execution by creating a batch directory and retrieving data
    # files with associated metadata.

    # create working directory for each batch
    batch_dir = work_dir / f'batch_{batch_id}_size_{size}_node_{node_id}'
    os.makedirs(batch_dir, exist_ok=True)

    # Retrieve genome files and associated metadata for each genome ID
    meta = {}
    for data_id in data_ids:
        data_file, tool_identifier = _retrieve_data_file(
            data_id,
            source_data_dir,
            source_file_ext,
            kbase_collection,
            tool_name,
            data_id_from_filename,
        )
        if data_file:
            meta[data_id] = {META_TOOL_IDENTIFIER: tool_identifier, META_SOURCE_FILE: data_file}

    return batch_dir, meta


def _retrieve_data_file(
    data_id: str,
    source_data_dir: Path,
    source_file_ext: str,
    collection: str,
    tool: str,
    data_id_from_filename: bool,
) -> tuple[Path, str]:
    # retrieve the data file associated with data_id
    # return the data file path if it exists, otherwise raise an error if collection is not
    # in the list of
    # IGNORE_MISSING_GENOME_FILES_COLLECTIONS

    data_file = _find_genome_file(data_id, source_file_ext, source_data_dir, collection)

    tool_identifier, genome_file = data_id, None
    # TODO DOWNLOADERS come up with a standard file name for the input file so we don't have to
    #                  worry about multiple files matching the extension
    if data_file:
        data_file = Path(data_files)
        if data_id_from_filename:
            # CheckM2 uses the base name (without extension) of genome_file as the genome
            # identifier
            tool_identifier = genome_file_name.split(source_file_ext)[0]

    return data_file, tool_identifier  # Intentionally returns None if not found for now


def _find_genome_file(
    genome_id: str,
    file_ext: str,
    source_data_dir: Path,
    collection: str,
) -> Path:
    # Finds genome file(s) for a given genome ID in a source data directory,
    # excluding files whose name contains specified substrings.

    # In most cases, our focus is solely on the {genome_id}.genomic.fna.gz file from NCBI,
    # while excluding {genome_id}.cds_from_genomic.fna.gz, {genome_id}.rna_from_genomic.fna.gz,
    # and {genome_id}.ERR_genomic.fna.gz files.
    genome_path = source_data_dir / genome_id

    if not genome_path.exists():
        raise ValueError(f'Cannot find file directory for: {genome_id}')

    genome_files = [genome_path / f for f in os.listdir(genome_path) if f.endswith(file_ext)]

    genome_files = [f for f in genome_files if
                    all(name_substr not in str(f)
                        for name_substr in STANDARD_FILE_EXCLUDE_SUBSTRINGS)]

    # Raise an error if no genome files are found and the collection is not in the
    # ignored collections
    if not genome_files and collection not in IGNORE_MISSING_GENOME_FILES_COLLECTIONS:
        raise ValueError(f'Cannot find target file(s) for: {genome_id}')

    # Raise an error if the number of files found does not match the expected file count
    if genome_files and len(genome_files) != 1:
        raise ValueError(f'Found {len(genome_files)} files for {genome_id} but expected 1')

    return genome_files[0]
