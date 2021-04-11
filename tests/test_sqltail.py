# sqltail tests

import sqltail

import datetime
import json
import logging
import os
import pytest

RUN_TIME=int(os.environ.get('RUN_TIME', '1'))

def test_import():
    import sqltail

@pytest.mark.skipif(not os.environ.get('DB_HOST'), reason='define DB_VARS to enable')
def test_init():
    db = sqltail.Database(suffix='_log')
    t = sqltail.SQLTail(db)
    assert t

@pytest.mark.skipif(not os.environ.get('DB_HOST'), reason='define DB_VARS to enable')
def test_run():
    db = sqltail.Database(suffix='_log')
    t = sqltail.SQLTail(db)
    start = datetime.datetime.now()
    ret = t.run(timeout=RUN_TIME)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= RUN_TIME 

@pytest.mark.skipif(not os.environ.get('DB_HOST'), reason='define DB_VARS to enable')
def test_template():
    result = sqltail.SQLTail(sqltail.Database(suffix='_log')).get_field_template()

@pytest.mark.skipif(not os.environ.get('DB_HOST'), reason='define DB_VARS to enable')
def test_fields():
    db = sqltail.Database(suffix='_log')
    fields=['timestamp', 'level', 'message']
    t = sqltail.SQLTail(db, fields=fields)
    start = datetime.datetime.now()
    ret = t.run(timeout=RUN_TIME)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= RUN_TIME 

@pytest.mark.skipif(not os.environ.get('DB_HOST'), reason='define DB_VARS to enable')
def test_filters():
    db = sqltail.Database(suffix='_log')
    filters=['level="INFO"']
    t = sqltail.SQLTail(db, filters=filters)
    start = datetime.datetime.now()
    ret = t.run(timeout=RUN_TIME)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= RUN_TIME

@pytest.mark.skipif(not os.environ.get('DB_HOST'), reason='define DB_VARS to enable')
def test_events():

    logging.basicConfig(level='DEBUG')
    db = sqltail.Database()
    template='[{"name":"session_id"},{"name":"form_id"},{"name":"event"},{"name":"detail"}]'
    t = sqltail.SQLTail(db, table='events', fields=json.loads(template))
    start = datetime.datetime.now()
    ret = t.run(timeout=RUN_TIME)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= RUN_TIME

