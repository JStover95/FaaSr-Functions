# What is FaaSr
FaaSr is a serverless middleware that replaces the low level idiosyncrasies of FaaS providers with DAG defined execution graphs. With FaaSr, it is easy to switch between serverless providers, making it easy to scale workflows without needing to worry about refactoring to new platforms. 

Currently, FaaSr supports GitHub actions, OpenWhisk, AWS Lambda, SLURM and Google Cloud. Functions within workflows can be written in Python or R and are ran inside of a FaaSr container on the user’s platform of choice. Workflows leverage S3 for persistent data-storage, with a built-in API for performing I/O within user functions.
This package provides backend tooling for DAG validation, compute server/data store checks, user package installation, function fetching and execution, workflow orchestration, and structured S3 logging. 

# Using
To use FaaSr, you simply need to create a workflow JSON (see below) and host your functions on GitHub. Then, you can register, invoke, and set triggers for your workflows using FaaSr's UI.

FaaSr abstracts away S3 interactions; all you need to do is use the FaaSr API within your functions.

```
faasr_get_file(local_file*, remote_file*, server_name, local_folder, remote_folder)
Downloads a file from specified S3 server to your local directory

faasr_put_file(local_file*, remote_file*, server_name, local_folder, remote_folder)
Uploads local_file to specified S3 server

faasr_delete_file(remote_file*, server_name, remote_folder)
Deletes remote_file from specified S3 server

faasr_log(msg*)
Logs a message to S3

faasr_get_folder_list(server_name, faasr_prefix)
Lists all of the objects in specified S3 server (within the faasr bucket) with prefix

faasr_get_s3_creds(server_name)
Returns S3 creds as a dict with the keys [bucket, region, endpoint, secret_key, access_key, anonymous]

faasr_rank()
Returns the rank and max_rank of the current function as a dict with the keys [rank, max_rank]
```
An * indicates that the parameter is required

Note: if you do not specify server_name, then your default data store will be used 

# Workflow builder
The GUI for creating a workflow can be found here: [FaaSr-JSON-Builder](https://owicky.github.io/faasr-workflow-builder/)

# Basic structure of an action in the workflow:
1. Workflow is validated
2. InvocationID is assigned if one is not present
3. User function is executed
4. Subsequent actions are invoked

# Prebuilt containers
### GitHub Actions
```
ghcr.io/faasr/github-actions-python:dev (Python)
ghcr.io/faasr/github-actions-r:dev (R)
```
### OpenWhisk
```
faasr/openwhisk-python:dev (Python)
faasr/openwhisk-r:dev (R)
```
### Google Cloud
```
faasr/gcp-python:dev (Python)
faasr/gcp-r:dev (R)
```
### Slurm
```
faasr/slurm-python:dev (Python)
faasr/slurm-r:dev (R)
```
### AWS Lambda
```
Email cutlern [at] oregonstate.edu or build your own
```

See [FaaSr-Docker](https://github.com/FaaSr/FaaSr-Docker) for building your own containers