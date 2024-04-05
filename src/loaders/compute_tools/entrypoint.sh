#!/bin/bash

# Get the value of the ENV_TOOL environment variable, or set it to "conda" if not set
env_tool="${ENV_TOOL:-conda}"
echo "using $env_tool to manage environment"

$env_tool run -n $CONDA_ENV \
  python $PY_SCRIPT \
  --load_ver $LOAD_VER \
  --source_ver $SOURCE_VER \
  --env $ENV \
  --kbase_collection $KBASE_COLLECTION \
  --root_dir $ROOT_DIR \
  --threads_per_tool_run $THREADS_PER_TOOL_RUN \
  --node_id $NODE_ID \
  --debug \
  --source_file_ext $SOURCE_FILE_EXT \
  --data_id_file $DATA_ID_FILE
