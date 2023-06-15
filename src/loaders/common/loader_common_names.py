"""
Module that provides a list of common names, along with their corresponding descriptions, to be utilized
by the loaders programs located in the src/loaders directory.
"""

# Arguments Descriptions

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
    f"KBase collection identifier name (default: {DEFAULT_KBASE_COLL_NAME})."
)

# Prefix for output directories generated by the compute tools.
COMPUTE_OUTPUT_PREFIX = "batch_"
COMPUTE_OUTPUT_NO_BATCH_PREFIX = "batch_no_batch_size_"

"""
File structure at NERSC for loader programs
"""

ROOT_DIR = (
    "/global/cfs/cdirs/kbase/collections"  # root directory for the collections project
)
SOURCE_DATA_DIR = (
    "sourcedata"  # subdirectory for all source data (e.g. GTDB genome files)
)
COLLECTION_DATA_DIR = "collectionsdata"  # subdirectory for collected data (e.g. computed genome attributes)

COLLECTION_SOURCE_DIR = "collectionssource"  # subdirectory for source collections

# metadata file generated in the tool result folder with tool generated genome identifier,
# original genome id and source genome file info
GENOME_METADATA_FILE = "genome_metadata.tsv"

# the name of the CSV file where we store the trait counts
TRAIT_COUNTS_FILE = 'trait_counts.csv'
SYS_TRAIT_ID = 'trait_id'  # unique identifier for a trait
DETECTED_GENE_SCORE_COL = 'detected_genes_score'  # column name for the detected genes score

# column name for the trait unique identifier defined in the granularity trait count table
MICROTRAIT_TRAIT_NAME = 'microtrait_trait-name'
UNWRAPPED_GENE_COL = 'unwrapped_genes'  # column name that contains parsed gene name from unwrapped rule

# kbase authentication token
KB_AUTH_TOKEN = "KB_AUTH_TOKEN"
# subdirectory for SDK jobs per user
SDK_JOB_DIR = "sdk_job_dir"
# used by the podman service. Note that the unix user ID must be interpolated into the string before use
DOCKER_HOST = "unix:/run/user/{}/podman/podman.sock"
# used by the callback server
KB_BASE_URL_DEFAULT = "https://kbase.us/services/"
# JSON keys in the download metadata file in a download directory
SOURCE_METADATA_FILE_KEYS = ["upa", "name", "type", "timestamp"]
# callback server docker image name
CALLBACK_IMAGE_NAME = "scanon/callback"

# genome_attribs
GENOME_ATTRIBS = "genome_attribs"
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

# The fatal error file created if a data file cannot be successfully processed
FATAL_ERROR_FILE = "fatal_error.json"

# Used by gtdb output summary files
GTDB_GENOME_ID_COL = "user_genome"
GTDB_CLASSIFICATION_COL = "classification"
GTDB_UNCLASSIFIED = "Unclassified"
GTDB_SUMMARY_FILE_PATTERN = "gtdbtk.*.summary.tsv"

# Used by the global fatal error file and nametuple 
FATAL_ID = "kbase_id"
FATAL_ERROR = "error"
FATAL_ERRORS = "errors"
FATAL_FILE = "file"
FATAL_TOOL = "tool"
FATAL_STACKTRACE = "stacktrace"