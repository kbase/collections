#!/bin/bash

# Get the value of the ENV_TOOL environment variable, or set it to "conda" if not set
env_tool="${ENV_TOOL:-conda}"
echo "using $env_tool to manage environment"

$env_tool run -n $CONDA_ENV \
  python /app/collections/src/loaders/genome_collection/compute_genome_attribs.py \
  --tools $TOOLS \
  --load_ver $LOAD_VER \
  --source_data_dir $SOURCE_DATA_DIR \
  --kbase_collection $KBASE_COLLECTION \
  --root_dir $ROOT_DIR \
  --threads $THREADS \
  --program_threads $PROGRAM_THREADS \
  --node_id $NODE_ID \
  --debug \
  --source_file_ext $SOURCE_FILE_EXT \
  --genome_id_file $GENOME_ID_FILE