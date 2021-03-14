# cli

import arrow
import click
import json
import logging
from pathlib import Path

from sqltail import SQLTail, Database, __version__

@click.command(name='sqltail')
@click.version_option()
@click.option('--host', envvar='DB_HOST', type=str)
@click.option('--port', envvar='DB_PORT', type=str)
@click.option('--user', envvar='DB_USER', type=str)
@click.option('--password', envvar='DB_PASSWORD', type=str)
@click.option('--database', envvar='DB_DATABASE', type=str)
@click.option('--config-file', default=None)
@click.option('--timeout', default=None, type=float)
@click.option('--interval', default=1, type=int)
@click.option('--timezone', envvar='TZ', default='UTC')
@click.option('--template', type=str, default=None, help='json field template, or @FILENAME containing same')
@click.option('--get-template', is_flag=True, help='output json field template')
@click.option('--get-columns', is_flag=True)
@click.option('-c', '--columns', default=None, type=str, help='comma delimited list of output column names')
@click.option('-f', '--filters', default=None, type=str, help='list of filter conditions') 
@click.option('-o', '--output-format', default='json', type=str, help='output format') 
@click.option('-l', '--log-level', envvar="LOG_LEVEL", default='WARNING', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], case_sensitive=False))

def sqltail(host, port, user, password, database, config_file, timeout, interval, timezone, columns, filters, log_level, get_template, get_columns, template, output_format):

    logging.basicConfig(level=log_level.upper())

    db=Database(host, port, user, password, database, config_file=config_file, debug=log_level=='DEBUG')
    columns = columns.split(',') if columns else []
    filters = filters.split(',') if filters else []
    if template:
        if template.startswith('@'):
            template = Path(template[1:]).read_text()
        columns = from_json(template)

    sql_tail = SQLTail(
        db, 
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
