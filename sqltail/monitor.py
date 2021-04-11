import arrow
import datetime
import logging
import time
import copy

"""
ideas:

    Instead of reconnecting and querying for max row ID,
    create a UDF on the server that does the query in a loop and sleeps, only returning data
    when the MAX ID has changed, or a timeout occurs,
    then we don't constantly create/destroy connections and cursors
    we sit in a select, and when the data returns, it's either a timeout or a new max-row-id

"""
    

TZ='UTC'
DATETIME_FIELD_NAMES = ['timestamp', 'created', 'updated']

WAIT_INTERVAL_INIT=0.1
WAIT_INTERVAL_MULTIPLIER=2
WAIT_INTERVAL_MAX=1

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
        self.sql_fields = ','.join([f for f in self.fields])
        self.logger.debug(f"{self}")

    def __str__(self):
        return f"{self.__class__.__name__}<{self.db} {self.table} {self.fields} {self.filters}"

    def init_fields(self, fields):
        ret = dict()
        column_map = {c.Field:c for c in self.columns}
        if not fields:
            fields = list(column_map.keys())
        elif isinstance(fields, str):
            fields = fields.split(',')
        elif isinstance(fields, list):
            if isinstance(fields[0],str):
                pass   # fields may be a list of column names
            elif isinstance(fields[0], dict):
                # fields is a list of field spec dicts
                for spec in fields:
                    field = Field(**spec)
                    ret[field.name]=field
                fields = None
            else:
                TypeError('Unable to interpret fields specification')
        else:
            raise TypeError('fields may be a list of either column names or field specifiers') 

        if fields:
            for field in fields:
                column = column_map[field]
                hint = 'datetime' if column.Type.startswith('datetime') else None
                ret[column.Field] = Field(column.Field, tz=self.tz, type_hint=hint)
        return ret 
            
    def get_columns(self):
        with self.db.cursor() as cursor:
            return cursor.query(f"DESCRIBE {self.table};")

    def get_field_template(self):
        return [field.template() for field in self.fields.values()]

    def run(self, timeout=None):
        self.logger.debug('run: begin')
        if timeout:
            timeout = arrow.utcnow() + datetime.timedelta(seconds=timeout)
        self.running = True
        last_id = self.get_last_row_id()
        wait_interval = WAIT_INTERVAL_INIT
        while self.running:
            self.db.cxn.reconnect()
            self.db.cxn.database = self.db.database
            self.logger.debug(f"Querying new rows since last_id {last_id}...")
            with self.db.cursor() as cursor:
                rows = self.get_new_rows(cursor, last_id)
            if len(rows):
                wait_interval = WAIT_INTERVAL_INIT
                self.logger.debug(f"{len(rows)} row{'' if len(rows)==1 else 's'} returned")
                self.output_rows(rows)
                last_id = rows[-1]._id
            elif timeout and (arrow.utcnow() > timeout):
                self.logger.debug('Timeout')
                self.running = False
            else:
                if wait_interval < WAIT_INTERVAL_MAX:
                    wait_interval *= WAIT_INTERVAL_MULTIPLIER
                else:
                    wait_interval = WAIT_INTERVAL_MAX
                self.logger.debug('No rows returned; sleeping...')
                time.sleep(wait_interval)
        self.logger.debug('run: end')

    def sql_where(self, where=None):
        ret = copy.copy(self.filters)
        if where:
            ret.append(where)
        return f"{'WHERE ' if ret else ''}{' AND '.join(ret)}"

    def get_new_rows(self, cursor, last_id):
        where = self.sql_where(f"id > {last_id}")
        rows = cursor.query(f"SELECT id as _id,{self.sql_fields} FROM {self.table} {where} ORDER BY id;")
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
        return self.delimiter.join([self.fields[k].fmt(v) for k,v in row.items() if k in self.fields])
        line = []
        for k,v in row.items():
            if k in self.fields:
                field = self.fields[k]
                func = field.fmt
                text = func(v)
                line.append(text)
        msg = self.delimiter.join(line)
        return msg


class Field():
    def __init__(self, name=None, format_func=None, where_clause=None, truncate=0, left_pad=0, right_pad=0, tz=TZ, type_hint=None):
        self.logger=logging.getLogger(__class__.__name__)
        if not name:
            raise ValueError("Field name cannot be None")
        self.name = name
        self.fmt = self.init_fmt_func(format_func, name, type_hint)
        self.where_clause = where_clause
        self.truncate = truncate
        self.lpad = left_pad
        self.rpad = right_pad
        self.tz = tz 
        self.logger.debug(f"{self}")

    def __str__(self):
        return f"{self.__class__.__name__}<{self.name} {self.fmt.__name__} {self.where_clause} {self.truncate} {self.lpad} {self.rpad} {self.tz}>"

    def template(self):
        return dict( 
                name=self.name, 
                format_func=self.fmt.__name__, 
                where_clause=self.where_clause, 
                truncate=self.truncate,
                left_pad=self.lpad,
                right_pad=self.rpad,
                tz=self.tz
            )

    def init_fmt_func(self, fmt_func, name, type_hint):
        if fmt_func:
            if isinstance(fmt_func, str):
                fmt = getattr(self, fmt_func)
            else:
                fmt = fmt_func
        else:
            if type_hint=='datetime' or name in DATETIME_FIELD_NAMES:
                fmt = self.fmt_datetime
            else:
                fmt = self.fmt_str
        return fmt

    def fmt_datetime(self, dt):
        return arrow.get(dt).to(self.tz).isoformat(' ')[:24]

    def fmt_str(self, value):
        value = str(value)
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
