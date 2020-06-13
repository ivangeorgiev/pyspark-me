# pyspark-me
Databricks client SDK for Python with command line interface for Databricks REST APIs.

[TOC]

## Introduction

Pysparkme package provides python SDK for Databricks REST API:

* dbfs
* workspace
* jobs
* runs

The package also comes with a useful CLI which might be very helpful in automation.

## Python Client SDK for Databricks REST APIs

### Create Databricks connection

```python
# Get Databricks workspace connection
dbc = pysparkme.databricks.connect(
        bearer_token='dapixyzabcd09rasdf',
        url='https://westeurope.azuredatabricks.net')
```

### DBFS

```python
# Get list of items at path /FileStore
dbc.dbfs.ls('/FileStore')

# Check if file or directory exists
dbc.dbfs.exists('/path/to/heaven')

# Make a directory and it's parents
dbc.dbfs.mkdirs('/path/to/heaven')

# Delete a directory recusively
dbc.dbfs.rm('/path', recursive=True)

# Download file block starting 1024 with size 2048
dbc.dbfs.read('/data/movies.csv', 1024, 2048)

# Download entire file
dbc.dbfs.read_all('/data/movies.csv')
```

### Databricks workspace

```python
# List root workspace directory
dbc.workspace.ls('/')

# Check if workspace item exists
dbc.workspace.exists('/explore')

# Check if workspace item is a directory
dbc.workspace.is_directory('/')

# Export notebook in default (SOURCE) format
dbc.workspace.export('/my_notebook')

# Export notebook in HTML format
dbc.workspace.export('/my_notebook', 'HTML')
```

## Databricks CLI `dbr-me`

You can call the Databricks CLI using convenient shell command `dbr-me`:

```bash
$ dbr-me --help
```

 or using python module:

```bash
$ python -m pysparkme.databricks.cli --help
```

To connect to the Databricks cluster, you can supply arguments at the command line:

* `--bearer-token`
* `--url`
* `--cluster-id`

Alternatively, you can define environment variables. Command line arguments take precedence.

```bash
export DATABRICKS_URL='https://westeurope.azuredatabricks.net/'
export DATABRICKS_BEARER_TOKEN='dapixyz89u9ufsdfd0'
export DATABRICKS_CLUSTER_ID='1234-456778-abc234'
export DATABRICKS_ORG_ID='87287878293983984'
```



### Workspace

```bash
####################
# List workspace
# Default path is root - '/'
dbr-me workspace ls
# auto-add leading '/'
dbr-me workspace ls 'Users'
# Space-indentend json output with number of spaces
dbr-me workspace --json-indent 4 ls
# Custom indent string
dbr-me workspace ls --json-indent='>'

#####################
# Export workspace items
# Export everything in source format using defaults: format=SOURCE, path=/
dbr-me workspace export -o ./.dev/export
# Export everything in DBC format
dbr-me workspace export -f DBC -o ./.dev/export.
# When path is folder, export is recursive
dbr-me workspace export -o ./.dev/export-utils 'Utils'
# Export single ITEM
dbr-me workspace export -o ./.dev/GetML 'Utils/Download MovieLens.py'
```



### DBFS

#### List DBFS items

```bash
# List items on DBFS
dbr-me dbfs ls --json-indent 3 FileStore/movielens
```

```bash
[
   {
      "path": "/FileStore/movielens/ml-latest-small",
      "is_dir": true,
      "file_size": 0,
      "is_file": false,
      "human_size": "0 B"
   }
]
```



```bash
# Download a file and print to STDOUT
dbr-me dbfs get ml-latest-small/movies.csv
```

```bash
# Download recursively entire directory and store locally
dbr-me dbfs get -o ml-local ml-latest-small
```



### Runs

#### Submit a notebook

Implements: [https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-submit](https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-submit)

```bash
$ dbr-me runs submit "Utils/Download MovieLens"
```

```
{"run_id": 4}
```

You can retrieve the job information using `runs get`:

```bash
$ dbr-me runs get 4 -i 3
```

#### Get run metadata

Implements: [Databricks REST runs/get](https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-get) 

```bash
$ dbr-me runs get -i 3 6
```

```json
{
   "job_id": 6,
   "run_id": 6,
   "creator_user_name": "your.name@gmail.com",
   "number_in_job": 1,
   "original_attempt_run_id": null,
   "state": {
      "life_cycle_state": "TERMINATED",
      "result_state": "SUCCESS",
      "state_message": ""
   },
   "schedule": null,
   "task": {
      "notebook_task": {
         "notebook_path": "/Utils/Download MovieLens"
      }
   },
   "cluster_spec": {
      "existing_cluster_id": "xxxx-yyyyy-zzzzzz"
   },
   "cluster_instance": {
      "cluster_id": "xxxx-yyyyy-zzzzzz",
      "spark_context_id": "783487348734873873"
   },
   "overriding_parameters": null,
   "start_time": 1592062497162,
   "setup_duration": 0,
   "execution_duration": 11000,
   "cleanup_duration": 0,
   "trigger": null,
   "run_name": "pyspark-me-1592062494",
   "run_page_url": "https://westeurope.azuredatabricks.net/?o=398348734873487#job/6/run/1",
   "run_type": "SUBMIT_RUN"
}
```



#### List Runs

Implements: [Databricks REST runs/list](https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-list)

```bash
$ dbr-me runs ls
```



To get only the runs for a particular job:

```bash
# Get job with job-id=4
$ dbr-me runs ls 4 -i 3
```

```json
{
   "runs": [
      {
         "job_id": 4,
         "run_id": 4,
         "creator_user_name": "your.name@gmail.com",
         "number_in_job": 1,
         "original_attempt_run_id": null,
         "state": {
            "life_cycle_state": "PENDING",
            "state_message": ""
         },
         "schedule": null,
         "task": {
            "notebook_task": {
               "notebook_path": "/Utils/Download MovieLens"
            }
         },
         "cluster_spec": {
            "existing_cluster_id": "xxxxx-yyyy-zzzzzzz"
         },
         "cluster_instance": {
            "cluster_id": "xxxxx-yyyy-zzzzzzz"
         },
         "overriding_parameters": null,
         "start_time": 1592058826123,
         "setup_duration": 0,
         "execution_duration": 0,
         "cleanup_duration": 0,
         "trigger": null,
         "run_name": "pyspark-me-1592058823",
         "run_page_url": "https://westeurope.azuredatabricks.net/?o=abcdefghasdf#job/4/run/1",
         "run_type": "SUBMIT_RUN"
      }
   ],
   "has_more": false
}
```



#### Export run 

Implements: [Databricks REST runs/export](https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-export)

```bash
$ dbr-me runs export --content-only 4 > .dev/run-view.html
```



#### Get run output

Implements: [Databricks REST runs/get-output](https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-get-output)

```bash
$ dbr-me runs get-output -i 3 6
```

```json
{
   "notebook_output": {
      "result": "Downloaded files: README.txt, links.csv, movies.csv, ratings.csv, tags.csv",
      "truncated": false
   },
   "error": null,
   "metadata": {
      "job_id": 5,
      "run_id": 5,
      "creator_user_name": "your.name@gmail.com",
      "number_in_job": 1,
      "original_attempt_run_id": null,
      "state": {
         "life_cycle_state": "TERMINATED",
         "result_state": "SUCCESS",
         "state_message": ""
      },
      "schedule": null,
      "task": {
         "notebook_task": {
            "notebook_path": "/Utils/Download MovieLens"
         }
      },
      "cluster_spec": {
         "existing_cluster_id": "xxxx-yyyyy-zzzzzzz"
      },
      "cluster_instance": {
         "cluster_id": "xxxx-yyyyy-zzzzzzz",
         "spark_context_id": "8973498743973498"
      },
      "overriding_parameters": null,
      "start_time": 1592062147101,
      "setup_duration": 1000,
      "execution_duration": 11000,
      "cleanup_duration": 0,
      "trigger": null,
      "run_name": "pyspark-me-1592062135",
      "run_page_url": "https://westeurope.azuredatabricks.net/?o=89798374987987#job/5/run/1",
      "run_type": "SUBMIT_RUN"
   }
}
```



To get only the exit output:

```bash
$ dbr-me runs get-output -r 6
```

```
Downloaded files: README.txt, links.csv, movies.csv, ratings.csv, tags.csv
```



## Build and publish

```bash
python setup.py sdist bdist_wheel
python -m twine upload dist/*
```