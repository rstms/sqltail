# sqltail tests

import sqltail

import datetime
import json
import logging
import os

RUN_TIME=int(os.environ.get('RUN_TIME', '10'))

def test_init():
    db = sqltail.Database(suffix='_log')
    t = sqltail.SQLTail(db)
    assert t

def test_run():
    db = sqltail.Database(suffix='_log')
    t = sqltail.SQLTail(db)
    start = datetime.datetime.now()
    ret = t.run(timeout=RUN_TIME)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= RUN_TIME 

def test_template():
    result = sqltail.SQLTail(sqltail.Database(suffix='_log')).get_field_template()

def test_fields():
    db = sqltail.Database(suffix='_log')
    fields=['timestamp', 'level', 'message']
    t = sqltail.SQLTail(db, fields=fields)
    start = datetime.datetime.now()
    ret = t.run(timeout=RUN_TIME)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= RUN_TIME 

def test_filters():
    db = sqltail.Database(suffix='_log')
    filters=['level="INFO"']
    t = sqltail.SQLTail(db, filters=filters)
    start = datetime.datetime.now()
    ret = t.run(timeout=RUN_TIME)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= RUN_TIME

def test_events():

    logging.basicConfig(level='DEBUG')
    db = sqltail.Database()
    template='[{"name":"session_id"},{"name":"form_id"},{"name":"event"},{"name":"detail"}]'
    t = sqltail.SQLTail(db, table='events', fields=json.loads(template))
    start = datetime.datetime.now()
    ret = t.run(timeout=RUN_TIME)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= RUN_TIME

