# cli

import arrow
import click
import json
import logging
import time
from pathlib import Path

from sqltail import SQLTail, Database, DatabaseNotFound, DatabaseConnectionFailed, __version__, __license__

@click.command(name='sqltail')
@click.version_option(message=f"sqltail v{__version__} {__license__}")
@click.option('--host', envvar='DB_HOST', type=str)
@click.option('--port', envvar='DB_PORT', type=str)
@click.option('--user', envvar='DB_USER', type=str)
@click.option('--password', envvar='DB_PASSWORD', type=str)
@click.option('--database', envvar='DB_DATABASE', type=str)
@click.option('--config-file', default=None)
@click.option('--timeout', default=None, type=float)
@click.option('--interval', default=1, type=int)
@click.option('--timezone', envvar='TZ', default='UTC')
@click.option('--template', type=str, default=None, help='json field template, or @FILENAME in cwd or ~/.sqltail')
@click.option('--get-template', is_flag=True, help='output json field template')
@click.option('--get-columns', is_flag=True)
@click.option('--suffix', type=str, default='_log', help="append SUFFIX to db name (defaults to _log)")
@click.option('-r/-R', '--retry/--no-retry', is_flag=True, default=True, help="retry on database connection failures")
@click.option('-t', '--table', default='log', type=str, help='table name')
@click.option('-c', '--columns', default=None, type=str, help='comma delimited list of output column names')
@click.option('-f', '--filters', default=None, type=str, help='list of filter conditions') 
@click.option('-o', '--output-format', default='json', type=str, help='output format') 
@click.option('-l', '--log-level', envvar="LOG_LEVEL", default='WARNING', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], case_sensitive=False))

def sqltail(host, port, user, password, database, config_file, timeout, interval, timezone, columns, filters, log_level, get_template, get_columns, table, template, output_format, suffix, retry):

    logging.basicConfig(level=log_level.upper())

    state = None
    while True:
        try:
            db=Database(host, port, user, password, database, config_file=config_file, debug=log_level=='DEBUG', suffix=suffix)
        except DatabaseNotFound as exc:
            if state != type(exc):
                click.echo(str(exc)+' retrying...')
                state = type(exc)
        except DatabaseConnectionFailed as exc:
            if state != type(exc):
                click.echo(str(exc)+' retrying...')
                state = type(exc)
        else:
            state = None
            break
        time.sleep(3)
        if not retry:
            click.exit(1)
        
    columns = columns.split(',') if columns else []
    filters = filters.split(',') if filters else []
    if template:
        if template.startswith('@'):
            template_path = Path(template[1:])
            if not template_path.is_file():
                template_path = Path.home() / '.sqltail' / template[1:]
            template = template_path.read_text()

        columns = from_json(template)

    sql_tail = SQLTail(
        db, 
        table=table,
        fields=columns, 
        filters=filters,
        callbacks=[output],
        tz=timezone,
        interval=interval
    )
    if get_template:
        output(sql_tail.get_field_template(), fmt=output_format)
    elif get_columns:
        output(sql_tail.get_columns(), fmt=output_format)
    else:
        sql_tail.run(timeout=timeout)

def to_json(data):
    if hasattr(data, '__iter__'):
        ret = '[\n  ' + ',\n  '.join([json.dumps(item) for item in data]) + '\n]'
    else:
        ret = json.dumps(data)
    return ret

def from_json(data):
    return json.loads(data) 

def output(msg, fmt=None):
    if fmt=='json':
        msg = to_json(msg)
    click.echo(msg)
