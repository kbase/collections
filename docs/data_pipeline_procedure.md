# Data Pipeline Procedure

## Set working directory to default group
   * Set working directory and its subdirectories to the default group
      ```commandline
      file_group=kbase
      find /global/cfs/cdirs/kbase/collections/working_dir -type d -exec chgrp $file_group {} \;
      ```
   * set the "setgid" bit so that anything created or modified in that direcotry will automatically be set to the default group
      ```commandline
      find /global/cfs/cdirs/kbase/collections/working_dir -type d -exec chmod g+s {} \;
      ```

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
   * Parse tool computation results
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
   * Parse genome taxa count
     * Example usage
       ```commandline
       python src/loaders/genome_collection/compute_genome_taxa_count.py \
         ../import_files/$env/GROW_2023.06_checkm2_gtdb_tk_kbcoll_genome_attribs.jsonl \
         --load_ver $load_ver \
         --kbase_collection $kbase_collection \
         --env $env \
         --root_dir $root_dir \
         --input_source genome_attributes
       ```
     * Generated files/metafiles
       * Parsed JSONL files for ArangoDB import
         ```text
         import_files/[env]/
         ```
   * Create PMI biolog heatmap data
     * Example usage
       * Download biolog heatmap data to designated `biolog_download_dir` directory
         * [biolog data file](https://docs.google.com/spreadsheets/d/1QmC6UHWOEVfpmrveRBl_izictbNeN1PA/edit#gid=1135979967)
         * [biolog metadata file](https://docs.google.com/spreadsheets/d/1A83PV9xNqtEn3REfDH0fNPGjo3CxuILl/edit#gid=1788949297)
       * in a Python shell run the following
         ```python
          from src.loaders.genome_collection.parse_PMI_biolog_data import generate_pmi_biolog_heatmap_data
         
          biolog_download_dir = 'xxx'
          biolog_data_file = Path(biolog_download_dir, 'PMI strain BiologSummary.xlsx')
          biolog_meta_file = Path(biolog_download_dir, 'genome_assembly_info__PMI_metadata_file_all_strains_table001.xlsx')
          load_ver = 'test_biolog'
        
          generate_pmi_biolog_heatmap_data(biolog_data_file, biolog_meta_file, load_ver)
         ```
       * Please be aware that when using a non-production environment, you must provide an additional `upa_map_file`
         that maps the UPAs in production to the other environment, since the biolog data is provided for production.
         The file content is a single JSON object that contains a dictionary with the production UPAs as keys and 
         the equivalent target environment UPAs as values.
         ```python
         upa_map_file = Path(biolog_download_dir, 'PMI_strain_upa_map.json')
         generate_pmi_biolog_heatmap_data(biolog_data_file, biolog_meta_file, load_ver, env=env, upa_map_file=upa_map_file)
         ```
   