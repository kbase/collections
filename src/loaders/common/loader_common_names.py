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
DEFAULT_KBASE_COLL_NAME = 'GTDB'
# Name for kbase collection argument
KBASE_COLLECTION_ARG_NAME = "kbase_collection"
# Description of the --kbase_collection argument in various loaders programs.
KBASE_COLLECTION_DESCR = f'KBase collection identifier name (default: {DEFAULT_KBASE_COLL_NAME}).'

"""
File structure at NERSC for loader programs
"""

ROOT_DIR = '/global/cfs/cdirs/kbase/collections'  # root directory for the collections project
SOURCE_DATA_DIR = 'sourcedata'  # subdirectory for all source data (e.g. GTDB genome files)
COLLECTION_DATA_DIR = 'collectionsdata'  # subdirectory for collected data (e.g. computed genome attributes)
