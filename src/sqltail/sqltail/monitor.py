import arrow
import datetime
import logging
import time
import copy

TZ='UTC'
DATETIME_FIELD_NAMES = ['timestamp', 'created', 'updated']

class SQLTail():
    def __init__(self, db, table='log', fields=[], filters=[], delimiter=' ', interval=1, callbacks=[print], tz=TZ):

        self.logger=logging.getLogger(__class__.__name__)

        self.db = db
        self.table = table
        self.tz = tz
        self.delimiter = delimiter
        self.interval = interval
        self.callbacks=callbacks
        self.filters = filters
        self.columns = self.get_columns()
        self.fields = self.init_fields(fields)
        self.sql_fields = ','.join([f.name for f in self.fields])
        self.logger.debug(f"{self}")

    def __str__(self):
        return f"{self.__class__.__name__}<{self.db} {self.table} {self.fields} {self.filters}"

    def init_fields(self, fields):
        ret = []
        column_map = {c.Field:c for c in self.columns}
        fields = fields or list(column_map.keys())
        for field in fields:
            column = column_map[field]
            fmt = Field.fmt_datetime if column.Type.startswith('datetime') else None
            ret.append(Field(column.Field, format_func=fmt, tz=self.tz))
        return ret 
            
    def get_columns(self):
        with self.db.cursor() as cursor:
            return cursor.query(f"DESCRIBE {self.table};")

    def run(self, timeout=None):
        self.logger.debug('run: begin')
        if timeout:
            timeout = arrow.utcnow() + datetime.timedelta(seconds=timeout)
        self.running = True
        last_id = self.get_last_row_id()
        while self.running:
            #self.db.cxn.reconnect()
            #self.db.cxn.database = self.db.database
            self.logger.debug(f"Querying new rows since last_id {last_id}...")
            with self.db.cursor() as cursor:
                rows = self.get_new_rows(cursor, last_id)
            if len(rows):
                self.logger.debug(f"{len(rows)} row{'' if len(rows)==1 else 's'} returned")
                self.output_rows(rows)
                last_id = rows[-1].id
            elif timeout and (arrow.utcnow() > timeout):
                self.logger.debug('Timeout')
                self.running = False
            else:
                self.logger.debug('No rows returned; sleeping...')
                time.sleep(self.interval)

        self.logger.debug('run: end')

    def sql_where(self, where=None):
        ret = copy.copy(self.filters)
        if where:
            ret.append(where)
        return f"{'WHERE ' if ret else ''}{' AND '.join(ret)}"

    def get_new_rows(self, cursor, last_id):
        where = self.sql_where(f"id > {last_id}")
        rows = cursor.query(f"SELECT {self.sql_fields} FROM {self.table} {where} ORDER BY id;")
        return rows

    def get_last_row_id(self):
        with self.db.cursor() as cursor:
            rows = cursor.query(f"SELECT id from {self.table} {self.sql_where()} ORDER BY id DESC LIMIT 1;")
        return rows[0].id

    def output_rows(self, rows):
        for msg in map(self.format_row, rows):
            for callback in self.callbacks:
                callback(msg)

    def format_row(self, row):
        return self.delimiter.join([self.fields[k].format_func(v) for k,v in row.items() if k in self.fields])

class Field():
    def __init__(self, name, format_func=None, where_clause=None, truncate=0, left_pad=0, right_pad=0, tz=TZ):
        self.logger=logging.getLogger(__class__.__name__)
        self.name = name
        self.fmt = format_func or self.init_fmt_func(name)
        self.where_clause = where_clause
        self.truncate = truncate
        self.lpad = left_pad
        self.rpad = right_pad
        self.tz = tz 
        self.logger.debug(f"{self}")

    def __str__(self):
        return f"{self.__class__.__name__}<{self.name} {self.fmt.__name__} {self.where_clause} {self.truncate} {self.lpad} {self.rpad} {self.tz}>"

    def init_fmt_func(self, name):
        if name == 'level':
            fmt = self.fmt_loglevel
        elif name in DATETIME_FIELD_NAMES:
            fmt = self.fmt_datetime
        else:
            fmt = self.fmt_str
        return fmt

    def fmt_loglevel(self, level):
        return logging.getLevelName(int(level))

    def fmt_datetime(self, dt):
        return arrow.Arrow(dt).to(self.tz).isoformat(' ')[:24]

    def fmt_str(self, value):
        if self.lpad:
            value = ' '*(self.lpad-len(value))+value
            if self.truncate:
                value = value[-self.truncate:]
        elif self.rpad:
            value = value+' '*(self.rpad-len(value))
            if self.truncate:
                value = value[:self.truncate]
        elif self.truncate:
            value = value[:self.truncate]
        return value
