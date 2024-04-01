# Creating and Activating a New Collection in the Collections Service

This document provides step-by-step instructions on how to create and activate a new collection within the 
Collections service. Before proceeding, ensure that you have the necessary permissions to create a new collection.

Please consult the [Data Pipeline Procedure](data_pipeline_procedure.md) document for instructions on preparing 
the data for the new collection if data is not yet available.

## Step 1: Verify Permissions
Execute the following command to verify if you have the required permissions:

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

## Step 2: Create (Save) a New Collection
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

## Step 3: Activate The New Collection

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