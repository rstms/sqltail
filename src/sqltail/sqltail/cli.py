# cli

import arrow
import click
import logging

from sqltail import SQLTail, Database, __version__

@click.command(name='sqltail')
@click.version_option()
@click.option('--host', envvar='DB_HOST', type=str)
@click.option('--port', envvar='DB_PORT', type=str)
@click.option('--user', envvar='DB_USER', type=str)
@click.option('--password', envvar='DB_PASSWORD', type=str)
@click.option('--database', envvar='DB_DATABASE', type=str)
@click.option('--config-file', default=None)
@click.option('--timeout', default=None, type=int)
@click.option('--interval', default=1, type=int)
@click.option('--timezone', envvar='TZ', default='UTC')
@click.option('-c', '--columns', default=None, type=str, help='list of output columns')
@click.option('-f', '--filters', default=None, type=str, help='list of filter conditions') 
@click.option('-l', '--log-level', envvar="LOG_LEVEL", default='WARNING', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], case_sensitive=False))
def sqltail(host, port, user, password, database, config_file, timeout, interval, timezone, columns, filters, log_level):

    logging.basicConfig(level=log_level.upper())

    db=Database(host, port, user, password, database, config_file=config_file, debug=log_level=='DEBUG')
    columns = columns.split(',') if columns else []
    filters = filters.split(',') if filters else []
    sql_tail = SQLTail(
        db, 
        fields=columns, 
        filters=filters,
        callbacks=[output],
        tz=timezone,
        interval=interval
    )
    sql_tail.run(timeout=timeout)

def output(msg):
    click.echo(msg)
