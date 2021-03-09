# sqltail tests

import sqltail.sqltail
import sqltail.db

import datetime

def test_init():
    db = sqltail.db.Database()
    t = sqltail.sqltail.SQLTail(db)
    assert t

def test_run():
    db = sqltail.db.Database()
    t = sqltail.sqltail.SQLTail(db)
    start = datetime.datetime.now()
    ret = t.run(timeout=3)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= 3

def test_fields():
    db = sqltail.db.Database()
    fields=['timestamp', 'level', 'message']
    t = sqltail.sqltail.SQLTail(db, fields=fields)
    start = datetime.datetime.now()
    ret = t.run(timeout=3)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= 3

def test_filters():
    db = sqltail.db.Database()
    filters=['level=10']
    t = sqltail.sqltail.SQLTail(db, filters=filters)
    start = datetime.datetime.now()
    ret = t.run(timeout=3)
    elapsed = (datetime.datetime.now() - start).seconds
    assert elapsed >= 3
