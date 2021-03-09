# sqltail tests

import sqltail

import datetime

RUN_TIME=10

def test_init():
    db = sqltail.Database()
    t = sqltail.SQLTail(db)
    assert t

def test_run():
    db = sqltail.Database()
    t = sqltail.SQLTail(db)
    start = datetime.datetime.now()
    ret = t.run(timeout=RUN_TIME)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= RUN_TIME 

def test_fields():
    db = sqltail.Database()
    fields=['timestamp', 'level', 'message']
    t = sqltail.SQLTail(db, fields=fields)
    start = datetime.datetime.now()
    ret = t.run(timeout=RUN_TIME)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= RUN_TIME 

def test_filters():
    db = sqltail.Database()
    filters=['level=10']
    t = sqltail.SQLTail(db, filters=filters)
    start = datetime.datetime.now()
    ret = t.run(timeout=RUN_TIME)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= RUN_TIME
