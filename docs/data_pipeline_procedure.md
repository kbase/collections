# Data Pipeline Procedure

## Set working directory to default group
   * Set working directory and its subdirectories to the default group
      ```commandline
      file_group=kbase
      working_dir=dir_to_set_group
      find /global/cfs/cdirs/kbase/collections/$working_dir -type d -exec chgrp $file_group {} \;
      ```
   * set the "setgid" bit so that anything created or modified in that direcotry will automatically be set to the default group
      ```commandline
      find /global/cfs/cdirs/kbase/collections/$working_dir -type d -exec chmod g+s {} \;
      ```

## Setting up Python environment using Pipenv
   * You will need python 3.11 as the python on the path.
     * The NERSC Python module offers a Python environment that comes pre-equipped with several commonly used Python packages.
       ```commandline
       module load python
       ```
       Alternatively, NERSC also provides a minimal conda installation that you can use to build your own custom conda environment.
       ```commandline
       module load conda
       conda create -n py311 python=3.11 pip
       conda activate py311
       ```
     * To confirm Python 3.11 is working, run:
       ```commandline
       python --version
       ```
     * If needed, explore various methods to configure Python environments at NERSC using the
       [NERSC Python documentation](https://docs.nersc.gov/development/languages/python/nersc-python/)

   * Install [Pipenv](https://pipenv.pypa.io/en/latest/)
     ```commandline
     pip install pipenv
     ```
   * Pull Latest Code from GitHub
     ```commandline
     cd /global/cfs/cdirs/kbase/collections/collections
     git pull origin main
     ```
   * Set up the Environment with Pipenv
      ```commandline
      pipenv sync --dev
      ```
   * Activate Pipenv shell
      ```commandline
      pipenv shell
      ```
   * Deactivate Pipenv shell
      ```commandline
      exit
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
   * Workspace Uploader
     * Example usage
        ```commandline
        python src/loaders/workspace_uploader/workspace_uploader.py \
          --workspace_id $workspace_id \
          --kbase_collection $kbase_collection \
          --source_ver $source_verion \
          --env $env \
          --token_filepath $token_filepath \
          --au_service_ver $au_service_ver \
          --load_id $load_id
        ```
     * Generated files/metafiles
       * NCBI source data directory
         ```text
         sourcedata/NCBI/NONE/<genome_name>/<file_name>
         ```
         Prior to running the workspace uploader, these directories should contain a GenBank file downloaded
         using the NCBI downloader script.
         A softlink is created by the downloader from the appropriate `collectionssource` directory (see below),
         and when an upload is complete a FASTA file and an `uploaded.yaml` file should be present.

       * WS source data directory
         ```text
         sourcedata/WS/[env]/[workspace_id]/[UPA]/[UPA].fa or [UPA].meta
         ```
         After an upload is complete, these directories should contain a FASTA file and a metadata file.
         This script generates the metadata file upon the successful upload of a genome object. 
         The FASTA file is hardlinked into the corresponding `collectionssource`directory, which is a
         softlink to a `sourcedata/WS` directory.

       * Softlinks for collections
         ```text
         collectionssource/NONE/[kbase_collection]/[source_ver]/[genome_id]/
         collectionssource/[env]/[kbase_collection]/[source_ver]/[UPA]/
         ```
         These directories are subsets of the `sourcedir` directories - currently either the `NCBI` directory
         or the `WS` directory. More data source directories may be added in future.
         The environment parameter `[env]` is one of the KBase environments, either `CI`, `NEXT`,
         `APPDEV`, or `PROD`, if the data source is from KBase. Otherwise the environment is `NONE`
         for non-KBase sources (currently only NCBI).
         
         The directories are always softlinks into the `sourcedir` directory structure. Effectively `sourcedir`
         acts like a cache, and establishing a new collection or new version of a collection just requires
         fetching any data that does not yet exist in `sourcedir` and then softlinking the `sourcedir` directories
         that are part of the collection. This prevents storing duplicate data that is otherwise shared between
         collections or collection versions.

         Following a successful upload of a genome object, the GenomeFileUtil will generate an associated
         FASTA file linked to the assembly object, which will be originally stored in the job data directory.
         Subsequently, the script will establish a hardlink for the FASTA file in both the collection source
         directory and the corresponding workspace object source directory. In addition, this script creates an
         uploaded.yaml file in the collection source directory containing the data to upload (the NONE
         environment directory) and a meta.yaml file in the uploaded data collection source directory
         (the directory with an environment and UPA).
         

       * KBase SDK job directory
         ```text
         sdk_job_dir/[username]/
         ```
         (preserved with `--keep_job_dir` option, otherwise removed automatically)
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
   