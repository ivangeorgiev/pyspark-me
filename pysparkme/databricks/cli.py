import os
import json
import click
from .connection import connect

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
    cluster_id = cluster_id or os.environ.get('CLUSTER_ID')

    _dbc = connect(bearer_token, url=url, cluster_id=cluster_id)

@cli.group(help='Databricks workspace commands')
def workspace():
    pass


@workspace.command(help='List Databricks workspace item(s)')
@click.argument('path')
@click.option('--json-indent', help='Number of spaces to use for JSON output indentation.')
def ls(path,json_indent):
    global _dbc
    path = path if len(path) and path[0] == '/' else '/' + path
    json_indent = int(json_indent) if isinstance(json_indent, str) and json_indent.isnumeric() else json_indent
    print(json.dumps(_dbc.workspace.ls(path), indent=json_indent, default=lambda o: o.__dict__()))


@workspace.command(help='Export Databricks workspace items')
@click.option('--format','-f', default='SOURCE', help='Export format: DBC, HTML, JUPYTER, SOURCE')
@click.option('--output', '-o', help='Output to a file or directory.')
@click.argument('path')
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
        # print(f'Export dir: {path}')
        items = _dbc.workspace.ls(path)
        for item in items:
            export_item(item)


    global _dbc
    path = path if len(path) and path[0] == '/' else '/' + path
    format = format.upper()

    is_recursive = True
    if format == 'DBC':
        is_recursive = False
        export_dbc(path)
    elif path == '/':
        export_dir(path)
    else:
        item = _dbc.workspace.ls(path)[0]
        is_recursive = item.is_directory
        export_item(item)

cli()
