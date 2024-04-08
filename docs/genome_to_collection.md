# From KBase Genome to Collection

This guide outlines the process of creating and activating a new collection from KBase narrative with Genome workspace
objects.

Note that for all commands you can run `--help` to get more information about the various CLI options.

## Step 1: NERSC Account Setup
The commands provided in this document are intended for execution within the terminals of 
[NERSC Perlmutter](https://docs.nersc.gov/systems/perlmutter/). 
If you don't have a NERSC account yet, you can create one via [Iris](https://docs.nersc.gov/iris/).
Before proceeding, please ensure that you have an active NERSC account and are logged in to Perlmutter.

```commandline
ssh perlmutter.nersc.gov
```

## Step 2: Prepare Source Data Files (FASTA)

To run the collections pipeline you will need data uploaded as Genome objects in a KBase Narrative. You can use the Narrative interface to upload data. Additionally, for NCBI data, there are command line based options provided below.

### If you already have a KBase narrative with target Genome objects
If you already possess a KBase narrative containing the desired Genome objects, download the FASTA files associated with
the Genome objects from the KBase workspace to NERSC. The following command facilitates this process:

```commandline
# update arguments as needed
workspace_id=69739
kbase_collection=ENIGMA
source_verion=2023.10
env=CI
token_filepath=path/to/token_file.txt  # This is a single-line file with the KBase token 

cd /global/cfs/cdirs/kbase/collections/collections
PYTHONPATH=. python src/loaders/workspace_downloader/workspace_downloader.py \
    --workspace_id $workspace_id \
    --kbase_collection $kbase_collection \
    --source_ver $source_verion \
    --root_dir $root_dir \
    --env $env \
    --token_filepath $token_filepath \
    --retrieve_sample 
```

### Download FASTA files from NCBI based on GTDB version
Alternatively, if you don't possess a KBase narrative, you can download FASTA files from NCBI based on GTDB version 
using the following command:

```commandline
# update arguments as needed
download_file_ext='genomic.fna.gz'
gtdb_release_ver=214

PYTHONPATH=. python src/loaders/ncbi_downloader/gtdb.py \
    --download_file_ext $download_file_ext \
    --release_ver $gtdb_release_ver
```

### Create KBase Genome objects from Genbank files
Optionally, you can also download Genbank files from NCBI and create KBase Genome objects from them using the 
following command. This process will also retrieve the FASTA file from the created Genome objects.

```commandline
# update arguments as needed

# download Genbank files from NCBI based on GTDB version
download_file_ext='genomic.gbff.gz'
gtdb_release_ver=214

PYTHONPATH=. python src/loaders/ncbi_downloader/gtdb.py \
    --download_file_ext $download_file_ext \
    --release_ver $gtdb_release_ver
    
# create KBase Genome objects from the downloaded Genbank files
workspace_id=72231
kbase_collection=GTDB
source_verion=$gtdb_release_ver
load_id=1
au_service_ver=dev
gfu_service_ver=dev
cbs_max_tasks=10

PYTHONPATH=. python src/loaders/workspace_uploader/workspace_uploader.py \
    --workspace_id $workspace_id \
    --kbase_collection $kbase_collection \
    --source_ver $source_verion \
    --env $env \
    --token_filepath $token_filepath \
    --au_service_ver $au_service_ver \
    --gfu_service_ver $gfu_service_ver \
    --cbs_max_tasks $cbs_max_tasks \
    --load_id $load_id  
```

# Step 3: Execute tools on FASTA files

Once the source data files are prepared, execute tools on these FASTA files. You can choose from various available 
tools such as gtdb_tk, checkm2, microtrait, or mash.

```commandline
# update arguments as needed
tool=gtdb_tk  # available tools: gtdb_tk, checkm2, microtrait, mash
load_ver=$source_verion
source_file_ext=.fa

PYTHONPATH=. python src/loaders/jobs/taskfarmer/task_generator.py \
    --tool $tool \
    --kbase_collection $kbase_collection \
    --source_ver $source_ver \
    --load_ver $load_ver \
    --source_file_ext $source_file_ext \
    --submit_job
```

# Step 4: Parse and Load Tool Outputs

## Parse tool results

### Parse tool computation results
Parsing tool results for extracting insights from the processed data. This step involves parsing tool computation 
results and genome taxa count information.

```commandline
# Parse tool computation results
PYTHONPATH=. python src/loaders/genome_collection/parse_tool_results.py \
    --kbase_collection $kbase_collection \
    --source_ver $source_ver \
    --load_ver $load_ver \
    --env $env
	
# Parse genome taxa count information
attri_file=${kbase_collection}_${load_ver}_checkm2_gtdb_tk_kbcoll_genome_attribs.jsonl
PYTHONPATH=. python src/loaders/genome_collection/compute_genome_taxa_count.py \
    ../import_files/$env/$kbase_collection/$load_ver/$attri_file \
    --load_ver $load_ver \
    --kbase_collection $kbase_collection \
    --env $env \
    --input_source genome_attributes
```

## Load parsed results to ArangoDB

```commandline
# set up an SSH tunnel (Not required when using an internal KBase machine such as dev03) 
USER_NAME=user_name                 # user name for login1.berkeley.kbase.us
FORWARD=localhost:48000
ssh -f -N -L $FORWARD:10.58.1.211:8531 \
 $USER_NAME@login1.berkeley.kbase.us

# execute arangoimport (Please ask system admin for the arangoDB credentials)
PARSED_FILE=json_file_path          # the file path generated by the parsing script
ARANGO_USER=arango_username         # arangoDB user name
ARANGO_PW=arango_password           # arangoDB password
ARANGO_DB=collections_dev           # arangoDB database name
ARANGO_COLL=kbcoll_genome_attribs   # arangoDB collection name

arangoimport --file $PARSED_FILE \
    --server.endpoint tcp://$FORWARD \
    --server.username $ARANGO_USER \
    --server.password $ARANGO_PW \
    --server.database $ARANGO_DB \
    --collection $ARANGO_COLL \
    --on-duplicate update
```

# Step 5: Create and Active the Collection

## 5.1: Verify Permissions
Before proceeding to create and activate the collection, execute the following command to verify if you have the 
required permissions:

```
# Set the server URL and associated token
SERVER='https://ci.kbase.us/services/collections'
TOKEN='your_token_here' 

curl -X 'GET' \
"$SERVER/whoami/" \
-H 'accept: application/json' \
-H "Authorization: Bearer $TOKEN"
```
Example response:
```
{"user":"tgu2","is_service_admin":true}
```
Ensure that the "is_service_admin" field is set to true in the response. If it's false or if you encounter any errors, 
contact system administrator to grant you the necessary permissions.

## 5.2: Create (Save) a New Collection
Utilize the `Save Collection` endpoint to create a new collection. Execute the following command to create a new collection:

```
COLL_ID='GTDB'
VER_TAG='r207.kbase.2'
DATA='{"key1": "value1", "key2": "value2"}'
curl -X 'PUT' \
  "$SERVER/collections/$COLL_ID/versions/$VER_TAG/" \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d "$DATA"
```

If a previous version of the collection exists with a different version tag, you can obtain the current activated
collection data by running the following command:
```
curl -X 'GET' \
  "$SERVER/collections/$COLL_ID/" \
  -H 'accept: application/json'
```

## 5.3: Activate The New Collection
There are two ways to activate a collection:

### Method 1: Activation by Version Tag
You can activate the collection using a specific version tag with the following command:

```
curl -X 'PUT' \
  "$SERVER/collections/$COLL_ID/versions/tag/$VER_TAG/activate/" \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN"
```

### Method 2: Activation by Version Number
Alternatively, you can activate the collection using its version number which is provided by the response of the 
`Save Collection` endpoint as the `ver_num`.

```
VERSION_NUM='14'
curl -X 'PUT' \
  "$SERVER/collections/$COLL_ID/versions/num/$VERSION_NUM/activate/" \
  -H 'accept: application/json' \
  -H "Authorization: Bearer $TOKEN"
```

Now, the new collection is successfully created and activated. You can access and verify the collection data via 
the KBase Collections UI ([CI](https://ci.kbase.us/collections)) or the Collections swagger API 
([CI](https://ci.kbase.us/services/collections/docs)). 