# test_sqlalchemy

import os
import time
import logging
from pprint import pprint as pp

from sqlalchemy import create_engine, MetaData, Table, desc 
from sqlalchemy.sql import select


#uri = f"mysql+mysqlconnector://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_DATABASE']}"
uri = f"mysql+mysqldb://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_DATABASE']}"

engine = create_engine(uri)
assert engine
meta = MetaData()
meta.reflect(bind=engine)

def test_tail_multi_connect():

    log = meta.tables['log']
    fields = (log.c.id, log.c.timestamp, log.c.level, log.c.message)

    with engine.connect() as connection:
        row = connection.execute(select((log.c.id,)).order_by(desc(log.c.id)).limit(1)).fetchone()
        max_id = row.id
        pp(dict(maxid=max_id))

    timeout = time.time() + 20
    while time.time() < timeout:
        with engine.connect() as connection:
            rows=True
            while rows:
                rows = connection.execute(select(fields).order_by(log.c.id).where(log.c.id>max_id)).fetchall()
                for row in rows:
                    pp([row.id, df(row.timestamp),logging.getLevelName(row.level), row.message[:64]])
                    max_id = row.id
        time.sleep(1)

def test_tail_single_connect():

    log = meta.tables['log']
    fields = (log.c.id, log.c.timestamp, log.c.level, log.c.message)

    with engine.connect() as connection:
        row = connection.execute(select((log.c.id,)).order_by(desc(log.c.id)).limit(1)).fetchone()
        max_id = row.id
        pp(dict(maxid=max_id))

        timeout = time.time() + 20
        while time.time() < timeout:
            rows=True
            while rows:
                rows = connection.execute(select(fields).order_by(log.c.id).where(log.c.id>max_id)).fetchall()
                for row in rows:
                    pp([row.id, df(row.timestamp),logging.getLevelName(row.level), row.message[:64]])
                    max_id = row.id
            time.sleep(1)

def df(dt):
    return dt.isoformat(' ')[:22]
