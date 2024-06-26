"""
Module that provides a list of common names, along with their corresponding descriptions, to be utilized
by the loaders programs located in the src/loaders directory.
"""

# Arguments Descriptions

# Name for root directory argument
ROOT_DIR_ARG_NAME = "root_dir"
# Description of the --root_dir argument in various loaders programs.
ROOT_DIR_DESCR = "Root directory for the collections project."

# Name for load version argument
LOAD_VER_ARG_NAME = "load_ver"
# Description of the --load_ver argument in various loaders programs.
LOAD_VER_DESCR = "KBase load version (e.g. r207.kbase.1)."

# The Default kbase collection identifier name
DEFAULT_KBASE_COLL_NAME = "GTDB"
# Name for kbase collection argument
KBASE_COLLECTION_ARG_NAME = "kbase_collection"
# Description of the --kbase_collection argument in various loaders programs.
KBASE_COLLECTION_DESCR = (
    f"KBase collection identifier name."
)

SOURCE_VER_ARG_NAME = "source_ver"
SOURCE_VER_DESCR = """Version of the source data, which should match the source directory in the collectionssource. 
(e.g. 207, 214 for GTDB, 2023.06 for GROW/PMI)"""

# Name for environment argument
ENV_ARG_NAME = "env"

# Prefix for output directories generated by the compute tools.
COMPUTE_OUTPUT_PREFIX = "job"
COMPUTE_OUTPUT_NO_BATCH = "_non_batch"

"""
File structure at NERSC for loader programs
"""
WS = "WS"  # workspace

ROOT_DIR = (
    "/global/cfs/cdirs/kbase/collections"  # root directory for the collections project
)
SOURCE_DATA_DIR = (
    "sourcedata"  # subdirectory for all source data (e.g. GTDB genome files)
)
COLLECTION_DATA_DIR = "collectionsdata"  # subdirectory for collected data (e.g. computed genome attributes)

COLLECTION_SOURCE_DIR = "collectionssource"  # subdirectory for source collections

# Default directory name for the parsed JSONL files for arango import
IMPORT_DIR = 'import_files'

# metadata file generated in the tool result folder with tool generated genome identifier,
# original genome id and source genome file info
GENOME_METADATA_FILE = "genome_metadata.tsv"

# the name of the CSV file where we store the trait counts
TRAIT_COUNTS_FILE = 'trait_counts.csv'

SYS_TRAIT_ID = 'trait_id'  # unique identifier for a trait
UNWRAPPED_GENE_COL = 'unwrapped_genes'  # column name that contains parsed gene name from unwrapped rule

# kbase authentication token
KB_AUTH_TOKEN = "KB_AUTH_TOKEN"
# subdirectory for SDK jobs per user
SDK_JOB_DIR = "sdk_job_dir"
# used by the podman service. Note that the unix user ID must be interpolated into the string before use
DOCKER_HOST = "unix:/run/user/{}/podman/podman.sock"
KB_ENV = ['CI', 'NEXT', 'APPDEV', 'PROD']
DEFAULT_ENV = 'NONE'  # default environment for non-kbase collections (e.g. GTDB)
# JSON keys in the download metadata file in a download directory
SOURCE_METADATA_FILE_KEYS = ["upa", "name", "type", "timestamp"]
# callback server docker image name
CALLBACK_IMAGE_NAME = "kbase/callback:test"  # TODO switch to kbase/callback:latest

# a list of IDs provided to the computation script
DATA_ID_COLUMN_HEADER = "genome_id"  # TODO DATA_ID change to data ID for generality

# The following headers are in the meta file written out by the computation script
META_DATA_ID = "data_id"
META_SOURCE_FILE = "source_file"
META_UNCOMPRESSED_FILE = "uncompressed_source_file"  # result of unzipping the original source file (deleted after use)
META_TOOL_IDENTIFIER = "tool_identifier"
META_SOURCE_DIR = "source_dir"
META_FILE_NAME = "meta_filename"

# filtering applied to list objects
OBJECTS_NAME_ASSEMBLY = "KBaseGenomeAnnotations.Assembly"
OBJECTS_NAME_GENOME = "KBaseGenomes.Genome"

# The metadata file name created during the Mash run
MASH_METADATA = 'mash_run_metadata.json'

# The metadata file name created during the Eggnog run
EGGNOG_METADATA = 'eggnog_run_metadata.json'

# The fatal error file created if a data file cannot be successfully processed
FATAL_ERROR_FILE = "fatal_error.json"

# Used by gtdb output summary files
GTDB_GENOME_ID_COL = "user_genome"
GTDB_CLASSIFICATION_COL = "classification"
GTDB_SUMMARY_FILE_PATTERN = "gtdbtk.*.summary.tsv"

# Used by the global fatal error file and nametuple 
FATAL_ERROR = "error"
FATAL_ERRORS = "errors"
FATAL_FILE = "file"
FATAL_TOOL = "tool"
FATAL_STACKTRACE = "stacktrace"

# key name for sample file and prepared sample file in the metadata file for downloaded workspace objects
# sample file contains raw sample information
SAMPLE_FILE_KEY = "sample_file"
# prepared sample file contains key-value pairs of parsed meta_controlled from node tree of the sample
SAMPLE_PREPARED_KEY = "sample_prepared_file"
SAMPLE_EFFECTIVE_TIME = "sample_effective_time"

# extension for source sample data file and prepared sample node data file for downloaded workspace objects
SAMPLE_FILE_EXT = "sample"
SAMPLE_PREPARED_EXT = "prepared.sample"

# TODO DOWNLOAD if we settle on a standard file name scheme for downloaders we can get
#               rid of this
STANDARD_FILE_EXCLUDE_SUBSTRINGS = ['cds_from', 'rna_from', 'ERR']

KB_BASE_URL_MAP = {'CI': 'https://ci.kbase.us/services/',
                   'NEXT': 'https://next.kbase.us/services/',
                   'APPDEV': 'https://appdev.kbase.us/services/',
                   'PROD': 'https://kbase.us/services/'}

# containers.conf path
CONTAINERS_CONF_PATH = "~/.config/containers/containers.conf"
# params in containers.conf file
CONTAINERS_CONF_PARAMS = {
    "seccomp_profile": "\"unconfined\"",
    "log_driver": "\"k8s-file\""
}
# field name for Kbase object metadata
FLD_KB_OBJ_UPA = "upa"
FLD_KB_OBJ_NAME = "name"
FLD_KB_OBJ_TYPE = "type"
FLD_KB_OBJ_TIMESTAMP = "timestamp"
FLD_KB_OBJ_GENOME_UPA = "genome_upa"

# map from workspace Genome object metadata name to the name displayed in the genome attributes table and its type
# the displayed name is prefixed with "kbase" to avoid name collision with tool generated attributes
KB_GENOME_ATTRI_PREFIX = "kbase"
GENOME_WS_META_NAME_MAP = {
    "Size": (f"{KB_GENOME_ATTRI_PREFIX}_genome_size", int),
    "GC content": (f"{KB_GENOME_ATTRI_PREFIX}_gc_content", float),
    "Number contigs": (f"{KB_GENOME_ATTRI_PREFIX}_num_contigs", int),
    "Number of CDS": (f"{KB_GENOME_ATTRI_PREFIX}_num_cds", int),
    "Number of Protein Encoding Genes": (f"{KB_GENOME_ATTRI_PREFIX}_num_protein_encoding_genes", int)
}
# identifier for the WS object info in the metadata file retrieved from the workspace downloader
ASSEMBLY_OBJ_INFO_KEY = "assembly_object_info"
GENOME_OBJ_INFO_KEY = "genome_object_info"

# FASTA file extension
# .fa files are generated by the workspace downloader with a fixed file extension (workspace_downloader._process_input)
# .fasta files are generated by workspace uploader as the default file extension from the GenomeFileUtil
FASTA_FILE_EXT = ['.fa', '.fasta']
