#!/bin/bash

# Check if micromamba is available
if command -v micromamba >/dev/null 2>&1; then
  echo "micromamba found, using micromamba"
  conda_cmd="micromamba"
# Check if conda is available
elif command -v conda >/dev/null 2>&1; then
  echo "conda found, using conda"
  conda_cmd="conda"
else
  echo "Neither conda nor micromamba found. Please install either conda or micromamba."
  exit 1
fi

$conda_cmd run -n $CONDA_ENV \
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