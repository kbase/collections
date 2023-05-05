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

# metadata file generated in the tool result folder with tool generated genome identifier,
# original genome id and source genome file info
GENOME_METADATA_FILE = "genome_metadata.tsv"

# the name of the CSV file where we store the trait counts
TRAIT_COUNTS_FILE = "trait_counts.csv"


# kbase authentication token
KB_AUTH_TOKEN = "KB_AUTH_TOKEN"
# subdirectory for SDK jobs per user
SDK_JOB_DIR = "sdk_job_dir"
# used by the podman service. Note that the unix user ID must be interpolated into the string before use
DOCKER_HOST = "unix:/run/user/{}/podman/podman.sock"
# used by the callback server
KB_BASE_URL_DEFAULT = "https://kbase.us/services/"
# JSON keys in the download metadata file in a download directory
JSON_KEYS = ["upa", "name", "type", "timestamp"]
# callback server docker image name
CALLBACK_IMAGE_NAME = "scanon/callback"
