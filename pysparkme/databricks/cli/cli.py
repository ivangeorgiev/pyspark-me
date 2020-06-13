import os
import sys
import base64
import json
import click
from ..connection import connect
from ..common import DatabricksLinkException


global _dbc


@click.group()
@click.option('--bearer-token', '-t', help='Bearer token. Default to DATABRICKS_BEARER_TOKEN environment variable')
@click.option('--url', '-u', help='Databricks URL. Default to DATABRICKS_URL environment variable')
@click.option('--cluster-id', '-c', help='Databricks cluster ID. Default to DATABRICKS_CLUSTER_ID environment variable')
@click.option('-v', help='Verbose 1')
@click.option('-vv', help='Verbose 2')
@click.option('-vvv', help='Verbose 3')
def cli(bearer_token, url, cluster_id, v, vv, vvv):
    global _dbc

    bearer_token = bearer_token or os.environ.get('DATABRICKS_BEARER_TOKEN', None)
    assert bearer_token, 'Bearer token is not provided'
    url = url or os.environ.get('DATABRICKS_URL')
    cluster_id = cluster_id or os.environ.get('DATABRICKS_CLUSTER_ID')

    _dbc = connect(bearer_token, url=url, cluster_id=cluster_id)

@cli.group(help='Databricks workspace commands')
def workspace():
    pass


@workspace.command(help='List Databricks workspace item(s)')
@click.argument('path', default='/', required=False)
@click.option('--json-indent', '-i', help='Number of spaces to use for JSON output indentation.')
def ls(path,json_indent):
    global _dbc
    path = path if path.startswith('/') else '/' + path
    json_indent = int(json_indent) if isinstance(json_indent, str) and json_indent.isnumeric() else json_indent
    try:
        print(json.dumps(_dbc.workspace.ls(path), indent=json_indent, default=lambda o: dict(o)))
    except DatabricksLinkException as exc:
        sys.stderr.write("{}\n".format(str(exc)))
    except Exception as exc:
        raise exc


@workspace.command(help='Export Databricks workspace items')
@click.option('--format','-f', default='SOURCE', help='Export format: DBC, HTML, JUPYTER, SOURCE')
@click.option('--output', '-o', help='Output to a file or directory.')
@click.argument('path', required=False, default='/')
def export(path, output, format):
    def save(item_path, content, item=None):
        languages = ['PYTHON', 'SCALA', 'R', 'SQL']
        extensions = ['.py', '.scala', '.r', '.sql']
        if not output or output == '-':
            print(content)
            return
        if is_recursive:
            rel_path = item_path[len(path):]
            to_path = '{}/{}'.format(output, rel_path)
            try:
                ext = extensions[languages.index(item.language)]
                if not to_path.endswith(ext):
                    to_path = to_path + ext
            except ValueError:
                ext = ''
            to_path = to_path
        else:
            to_path = output
        abs_path = os.path.abspath(to_path)
        dirname = os.path.dirname(abs_path)
        os.makedirs(dirname, exist_ok=True)
        with open(abs_path, 'wb') as fh:
            fh.write(content)

    def export_item(item):
        if item.is_directory:
            export_dir(item.path)
        elif item.is_notebook:
            export_notebook(item)

    def export_dbc(path):
        save(path, _dbc.workspace.export(path, format))

    def export_notebook(item):
        save(item.path, _dbc.workspace.export(item.path, format), item)

    def export_dir(path):
        items = _dbc.workspace.ls(path)
        for item in items:
            export_item(item)


    global _dbc
    path = path if len(path) and path[0] == '/' else '/' + path
    format = format.upper()

    try:
        is_recursive = True
        if format == 'DBC':
            is_recursive = False
            export_dbc(path)
        elif path == '/':
            export_dir(path)
        else:
            item = _dbc.workspace.ls(path)[0]
            if item.path == path and not item.is_directory:
                is_recursive = False
                export_item(item)
            else:
                is_recursive = True
                export_dir(path)
    except DatabricksLinkException as exc:
        sys.stderr.write("{}\n".format(str(exc)))
    except Exception as exc:
        raise exc

@cli.group(help='Databricks DBFS commands')
def dbfs():
    pass


@dbfs.command(help='List DBFS item(s)')
@click.argument('path', required=False, default='/', type=str)
@click.option('--json-indent', help='Number of spaces to use for JSON output indentation.')
def ls(path, json_indent):
    global _dbc
    path = path if len(path) and path[0] == '/' else '/' + path
    json_indent = int(json_indent) if isinstance(json_indent, str) and json_indent.isnumeric() else json_indent

    try:
        result = _dbc.dbfs.ls(path)
        print(json.dumps(result, indent=json_indent, default=lambda o: dict(o)))
    except DatabricksLinkException as exc:
        sys.stderr.write("{}\n".format(str(exc)))
    except Exception as exc:
        raise exc



@dbfs.command(help='Download a file from DBFS')
@click.option('--chunk-size', '-c', default=1024*1024, help='Download chunk size')
@click.option('--output', '-o', default='-', help='Output file. Default is "-", writing to stdout')
@click.argument('path')
def get(path, output, chunk_size):
    def get_item(item, recursive):
        if item.is_file:
            file_path = output
            if recursive and output != '-':
                rel_path = item.path[len(path):]
                file_path = os.path.abspath('{}/{}'.format(output, rel_path))
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
            get_file(item.path, file_path)
        else:
            for dir_item in _dbc.dbfs.ls(item.path):
                get_item(dir_item, recursive)

    def get_file(path, output):
        fh = sys.stdout.buffer if output == '-' else open(output, 'wb')
        try:
            offset = 0
            while (True):
                this_read = _dbc.dbfs.read(
                        path, 
                        offset=offset,
                        length=chunk_size,
                        decoded=False)
                if not this_read['bytes_read']:
                    break
                offset += this_read['bytes_read']
                content = base64.b64decode(this_read['data'])
                fh.write(content)
        finally:
            if fh is not sys.stdout.buffer:
                fh.close()


    global _dbc
    path = path if len(path) and path[0] == '/' else '/' + path
    
    try:
        item = _dbc.dbfs.info(path)
        get_item(item, item.is_dir)
    except DatabricksLinkException as exc:
        sys.stderr.write("{}\n".format(str(exc)))
    except Exception as exc:
        raise exc

@dbfs.command(help='Remove a file or directory from DBFS')
@click.option('--recursive', '-r', is_flag=True, help='Remove recursively')
@click.argument('path')
def rm(path, recursive):
    global _dbc
    path = path if len(path) and path[0] == '/' else '/' + path

    try:
        _dbc.dbfs.delete(path, recursive)
    except DatabricksLinkException as exc:
        sys.stderr.write("{}\n".format(str(exc)))
    except Exception as exc:
        raise exc

@dbfs.command(help='Remove a file or directory from DBFS')
@click.argument('path')
def mkdirs(path):
    global _dbc
    path = path if len(path) and path[0] == '/' else '/' + path

    try:
        _dbc.dbfs.mkdirs(path)
    except DatabricksLinkException as exc:
        sys.stderr.write("{}\n".format(str(exc)))
    except Exception as exc:
        raise exc

# ######################## Jobs ###########################

@cli.group(help='Databricks jobs commands')
def jobs():
    pass


@jobs.command(help='List databricks jobs')
@click.argument('path', required=False, default='/', type=str)
@click.option('--json-indent', help='Number of spaces to use for JSON output indentation.')
def ls(path, json_indent):
    global _dbc
    path = path if path.startswith('/') else '/' + path
    json_indent = int(json_indent) if isinstance(json_indent, str) and json_indent.isnumeric() else json_indent

    try:
        result = _dbc.jobs.list(path)
        print(json.dumps(result, indent=json_indent, default=lambda o: dict(o)))
    except DatabricksLinkException as exc:
        sys.stderr.write("{}\n".format(str(exc)))
    except Exception as exc:
        raise exc


# ######################## Runs ###########################

@cli.group(help='Databricks runs commands')
def runs():
    pass


@runs.command()
@click.argument('job-id', required=False, type=str)
@click.option('--completed-only', is_flag=True, help='List only completed runs.')
@click.option('--active-only', is_flag=True, help='List only active runs.')
@click.option('--offset', help='Page offset.')
@click.option('--limit', help='Page length.')
@click.option('--json-indent', '-i', help='Number of spaces to use for JSON output indentation.')
def ls(job_id=None, completed_only=False, active_only=False, offset=None, limit=None, json_indent=None):
    """
    List runs from most recently started to least.

    See: https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-list
    """
    global _dbc
    json_indent = int(json_indent) if isinstance(json_indent, str) and json_indent.isnumeric() else json_indent

    try:
        result = _dbc.jobs.runs.ls(job_id=job_id, completed_only=completed_only, active_only=active_only,
                offset=offset, limit=limit)
        print(json.dumps(result, indent=json_indent, default=lambda o: dict(o)))
    except DatabricksLinkException as exc:
        sys.stderr.write("{}\n".format(str(exc)))
    except Exception as exc:
        raise exc

@runs.command()
@click.argument('path', required=True, type=str)
@click.option('--run-name', '-n', help='Run name')
@click.option('--parameters', '-p', type=str, help='Parameters JSON object.')
@click.option('--cluster-id', '-c', help='Cluster ID. Default from DATABRICKS_CLUSTER_ID environment variable.')
@click.option('--json-indent', help='Number of spaces to use for JSON output indentation.')
def submit(path, run_name=None, parameters=None, cluster_id=None, json_indent=None):
    '''
    Submit a one-time run. Doesn’t require a Databricks job to be created.

    See: https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-submit
    '''
    global _dbc
    path = path if path.startswith('/') else '/' + path
    json_indent = int(json_indent) if isinstance(json_indent, str) and json_indent.isnumeric() else json_indent

    if parameters:
        if parameters.startswith('@'):
            with open(parameters[1:], 'r', encoding='utf8') as f:
                parameters = f.read()
        params = json.loads(parameters)
    else:
        params = None

    try:
        result = _dbc.jobs.runs.submit_notebook(path=path, params=params, run_name=run_name)
        print(json.dumps({'run_id': result}, indent=json_indent, default=lambda o: dict(o)))
    except DatabricksLinkException as exc:
        sys.stderr.write("{}\n".format(str(exc)))
    except Exception as exc:
        raise exc


@runs.command()
@click.argument('run-id', required=True, type=int)
@click.option('--json-indent', '-i', help='Number of spaces or string to use for JSON output indentation.')
# https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-get
def get(run_id=None, json_indent=None):
    '''
    Retrieve the metadata of a run.

    See: https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-get
    '''
    global _dbc
    json_indent = int(json_indent) if isinstance(json_indent, str) and json_indent.isnumeric() else json_indent

    try:
        result = _dbc.jobs.runs.get(run_id=run_id)
        print(json.dumps(result, indent=json_indent, default=lambda o: dict(o)))
    except DatabricksLinkException as exc:
        sys.stderr.write("{}\n".format(str(exc)))
    except Exception as exc:
        raise exc

@runs.command()
@click.argument('run-id', required=True, type=int)
@click.option('--content-only', '-c', is_flag=True, help="Content only.")
@click.option('--json-indent', '-i', help='Number of spaces or string to use for JSON output indentation.')
# https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-get
def export(run_id=None, content_only=False, json_indent=None):
    '''
    Export and retrieve the job run task.

    See: https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-export
    '''
    global _dbc
    json_indent = int(json_indent) if isinstance(json_indent, str) and json_indent.isnumeric() else json_indent

    try:
        result = _dbc.jobs.runs.export(run_id=run_id)
        if content_only:
            for item_otput in result.views:
                print(item_otput.content)
        else:
            print(json.dumps(result.views, indent=json_indent, default=lambda o: dict(o)))
    except DatabricksLinkException as exc:
        sys.stderr.write("{}\n".format(str(exc)))
    except Exception as exc:
        raise exc


@runs.command()
@click.argument('run-id', required=True, type=int)
@click.option('--result-only', '-r', is_flag=True, help="Content only.")
@click.option('--json-indent', '-i', help='Number of spaces or string to use for JSON output indentation.')
# https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-get
def get_output(run_id=None, result_only=False, json_indent=None):
    '''
    Retrieve the output of a run.

    When a notebook task returns a value through the dbutils.notebook.exit() call, 
    you can use this endpoint to retrieve that value. Databricks restricts this API 
    to return the first 5 MB of the output. For returning a larger result, you can 
    store job results in a cloud storage service.

    See: https://docs.databricks.com/dev-tools/api/latest/jobs.html#runs-export
    '''
    global _dbc
    json_indent = int(json_indent) if isinstance(json_indent, str) and json_indent.isnumeric() else json_indent

    try:
        result = _dbc.jobs.runs.get_output(run_id=run_id)
        if result_only:
            print(result.notebook_output.result)
        else:
            print(json.dumps(result, indent=json_indent, default=lambda o: dict(o)))
    except DatabricksLinkException as exc:
        sys.stderr.write("{}\n".format(str(exc)))
    except Exception as exc:
        raise exc

