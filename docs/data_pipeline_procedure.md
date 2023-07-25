# Data Pipeline Procedure

## Download Source Data
   * Workspace Downloader
     * Example usage
        ```commandline
        python src/loaders/workspace_downloader/workspace_downloader.py \
          --env $env \
          --workspace_id $workspace_id \
          --kbase_collection $kbase_collection \
          --source_ver $source_verion \
          --token_filepath $token_filepath \
          --retrieve_sample 
        ```
     * Generated files/metafiles
       * downloaded files
         ```text
         sourcedata/WS/[env]/[workspace_id]/[UPA]/
         ```
       * Metadata file
         ```text
         sourcedata/WS/[env]/[workspace_id]/[UPA]/[UPA].meta
         ```
       * Softlinks for collections
         ```text
         collectionssource/[env]/[kbase_collection]/[source_ver]/[UPA]/
         ```
       * KBase SDK job directory
         ```text
         sdk_job_dir/[username]/
         ```
         (preserved with `--keep_job_dir` option, otherwise removed automatically)

   * NCBI Downloader
     * Example usage
       ```commandline
       python src/loaders/ncbi_downloader/gtdb.py \
         --download_file_ext $download_file_ext \
         --release_ver $release_ver
       ```
     * Generated files/metafiles
       * downloaded files
         ```text
         sourcedata/NCBI/NONE/[ncbi_genome_id]/
         ```
       * Metadata file
         ```text
         None
         ```
       * Softlinks for collections
         ```text
         collectionssource/NONE/GTDB/[GTDB_release_ver]/[ncbi_genome_id]/
         ```
## Schedule Taskfarmer Jobs
   * Example usage
     ```commandline
     python src/loaders/jobs/taskfarmer/task_generator.py \
       --tool $tool \
       --env $env \
       --kbase_collection $kbase_collection \
       --source_ver $source_ver \
       --load_ver $load_ver \
       --source_file_ext $source_file_ext \
       --submit_job
     ```
   * Generated files/metafiles
     * Taskfarmer job files
       ```text
       task_farmer_jobs/[job_dir]/
       ```
       * job_dir is formatted as `[kbase_collection]_[load_ver]_[tool]`
       * including files utilized by TaskFarmer
         * [shifter_wrapper.sh](https://docs.nersc.gov/jobs/workflow/taskfarmer/#step-1-write-a-wrapper-wrappersh)
         * [submit_taskfarmer.sl](https://docs.nersc.gov/jobs/workflow/taskfarmer/#step-3-create-a-batch-script-submit_taskfarmersl)
         * [tasks.txt](https://docs.nersc.gov/jobs/workflow/taskfarmer/#step-2-create-a-task-list-taskstxt)
         * genome_id_x.tsv: utilized by tool compute script to retrieve source file, which is generated per task
         * result/log files created by TaskFarmer
     * Taskfarmer jobs tracking file
       ```text
       task_farmer_jobs/task_info.jsonl
       ```
     * Tool result files
       ```text
       collectionsdata/[env]/[kbase_collection]/[load_ver]/[tool]/[batch_dir]/
       ```
       (`mash` outputs are saved in the source file directory)
     * Tool result metadata file
       ```text
       collectionsdata/[env]/[kbase_collection]/[load_ver]/[tool]/[batch_dir]/genome_metadata.tsv
       ```
## Parse Tool Results
   * Example usage
     ```commandline
     python src/loaders/genome_collection/parse_tool_results.py \
       --env $env \
       --kbase_collection $kbase_collection \
       --source_ver $source_ver \
       --load_ver $load_ver
     ```
   * Generated files/metafiles
     * Parsed JSONL files for ArangoDB import
       ```text
       import_files/[env]/
       ```