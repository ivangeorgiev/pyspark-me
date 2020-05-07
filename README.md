# pyspark-me
Pyspark and Databricks tools for everyday life

## Synopsis

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

## Databricks CLI

Get CLI help
```bash
python -m pysparkme.databricks.cli --help
```

Export the whole Databricks workspace into a directory `explore/export`.
Databricks token is taken from `DATABRICKS_BEARER_TOKEN`  environment variable.

```
python -m pysparkme.databricks.cli workspace export -o explore/export ''
```
### DBFS

```bash
# List items on DBFS
python -m pysparkme.databricks.cli dbfs ls --json-indent 2 ''
```

```bash
# Download a file and print to STDOUT
python -m pysparkme.databricks.cli dbfs get ml-latest-small/movies.csv
```

```bash
# Download recursively entire directory and store locally
python -m pysparkme.databricks.cli dbfs get -o ml-local ml-latest-small
```

## Build and publish

```bash
python setup.py sdist bdist_wheel
python -m twine upload dist/*
```