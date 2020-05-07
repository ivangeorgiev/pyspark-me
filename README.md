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

## Build and publish

```bash
python setup.py sdist bdist_wheel
python -m twine upload dist/*
```