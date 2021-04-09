# sqltail db

import dotmap
import logging
import mysql.connector
import os
import sys
import traceback
import configparser
import pathlib

class DatabaseException(Exception):
    """base class for Database exceptions""" 
    pass
               
class DatabaseConnectionFailed(DatabaseException):
    pass

class DatabaseNotFound(Exception):
    """Raised when the expected database is not present."""
    pass


class Database():

    def __init__(self, host=None, port=None, user=None, password=None, database=None, config_file=None, debug=False, verbose=False, suffix=None):

        self.cxn = None
        self.debug = debug
        self.verbose = verbose 
        self.logger = logging.getLogger(self.__class__.__name__)
        if debug:
            self.logger.setLevel(logging.DEBUG)
        self.cfg=self.init_config(config_file)
        self.host = self.get_parameter(host, 'DB_HOST', 'client', 'host')
        self.port = self.get_parameter(port, 'DB_PORT', 'client', 'port')
        self.user = self.get_parameter(user, 'DB_USER', 'client', 'user')
        self.password = self.get_parameter(password, 'DB_PASSWORD', 'client', 'password')
        self.database = self.get_parameter(database, 'DB_DATABASE', 'client', 'database')
        if suffix:
            self.database += suffix

        # connect without specifying a database
        if not self.connect(database=None):
            raise DatabaseConectionFailed(f"Failed connection to database {self.connection_string}")

        # verify the expected database is present
        with self.cursor(tuple=True) as cursor:
            rows = cursor.query('show databases;')
            available_databases = [r[0] for r in rows]
        self.logger.debug(f"databases found: {repr(available_databases)}")

        if not self.database in available_databases:
            raise DatabaseNotFound(f"Database {self.database} is not present.")

        # specify the database we need
        self.cxn.database = self.database;

        self.logger.debug(f"{self}")

    def init_config(self, filename):
        cfg = configparser.ConfigParser()
        if filename:
            path = [pathlib.Path(filename)]
        else:
            path = [
                pathlib.Path().home() / '.sqltail',
                pathlib.Path().home() / '.my.cnf'
            ]
        cfg.read(path)
        return cfg

    def get_parameter(self, value, env, section, key):
        if not value:
            value = os.environ.get(env)
            if not value:
                value = self.cfg[section][key]
        return value


    def __enter__(self):
        return self

    def __exit__(self, etype, value, tb):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        if self.cxn:
            self.cxn.close()
            self.cxn = None

    def __str__(self):
        """return string representaton for this instance"""
        return f'{self.__class__.__name__}<{self.connection_string} {self.cxn}>'

    def cursor(self, **kwargs):
        """return a Cursor configured with the database connection"""
        return Cursor(self, **kwargs)

    def connect(self, database=None):
        self.connection_string = f"mysql://{self.user}@{self.host}:{self.port}/{self.database if self.database else ''}"
        self.cxn = mysql.connector.connect(
            host=self.host, port=int(self.port), user=self.user, password=self.password, database=database,
            consume_results=True
        )
        self.cxn.get_warnings = True
        return self.cxn


"""
The Cursor() class wraps the mysql.connector.connection.cursor() class in a
context manager.  It supports execute() and query() functions, with local
exception handling passing diagnostic and debugging data to the logging 
system on errors.

Example: 

    with Cursor(db) as cursor:
        rows = cursor.execute('show databases;')

    for row in rows:
        print(repr(row))
    
"""


class Cursor():
    def __init__(self, db, **kwargs):
        """
        Cursor() Constructor

        :db: the parent Database instance
        :param **kwargs: keyword arguments (see below)
        :return: returns nothing
        
        The following keyword arguments may be passed boolean values:

        commit - call commit() after each statement
        ignore_warnings - query() function will request and log MySQL warnings
        ignore_notes - query() function will ignore 'Note' type warnings
        dictionary - query() will return rows as type dict
        tuple - query() will return rows as type tuple
        buffered - passed to the cursor() constructor to modify its function (see MySQL documentation)
        """
        self.db = db
        self.logger = logging.getLogger(self.__class__.__name__)
        if self.db.debug:
            self.logger.setLevel(logging.DEBUG)
        self.sql = ''
        self.lastrowid = None
        self.commit = kwargs.get('commit', False)
        self.ignore_warnings = kwargs.get('ignore_warnings', False)
        self.ignore_notes = kwargs.get('ignore_notes', True)
        self.buffered = kwargs.get('buffered', False)
        self.dictionary = kwargs.get('dictionary', False)
        self.tuple = kwargs.get('tuple', False)
        self.return_rows = True
        if self.tuple:
            self.dictionary = False
            self.return_rows = False 
        elif self.dictionary:
            self.return_rows = False 
        else:
            self.dictionary = True
            self.return_rows = True
        self.cursor = self.db.cxn.cursor(dictionary=self.dictionary, buffered=self.buffered)
        self.logger.debug(f"{self}")

    def __str__(self):
        return f"{__class__.__name__}{self.cursor}"

    def __enter__(self):
        return self

    def __exit__(self, etype, value, tb):
        if self.commit and self.db.cxn.in_transaction:
            self.db.cxn.commit()
        self.cursor.close()

    def _execute(self, *args, **kwargs):
        sql = repr(args)
        self.with_rows = False
        self.column_names = None
        self.description = None
        self.lastrowid = None
        self.rowcount = 0
        self.statement=None
        try:
            result = self.cursor.execute(*args, **kwargs)
            assert result == None
            self.with_rows = self.cursor.with_rows
            self.column_names = self.cursor.column_names
            self.description = self.cursor.description
            self.lastrowid = self.cursor.lastrowid
            self.rowcount = self.cursor.rowcount
            self.statement = self.cursor.statement
            if self.commit and self.db.cxn.in_transaction:
                self.db.cxn.commit()
        except mysql.connector.Error as e:
            f = traceback.extract_stack()[-3]
            self.logger.error(f"SQL Error {e} caller={f.filename}:{f.lineno} query={sql}")
            if self.db.verbose:
                self._dump_sql_error(*args)
            raise e
        return self.cursor

    def _dump_sql_error(self, sql):
        sys.stderr.write('SQL_ERROR_IN:\n' + sql + '\n')
        sys.stderr.flush()

    def execute(self, *args, **kwargs):
        self.logger.debug(f"{self} {args} {kwargs}")
        ret = self._execute(*args, **kwargs)
        self.logger.debug(f"{self} returning {ret}")
        return self 

    def query(self, *args, **kwargs):
        self.logger.debug(f"{self} {args} {kwargs}")
        rows = self._execute(*args, **kwargs).fetchall()
        if not self.ignore_warnings:
            self.handle_warnings()
        if self.return_rows:
            rows = [Row(row) for row in rows]
        self.logger.debug(f"{self} returning {len(rows)} {'row' if len(rows)==1 else 'rows'}")
        if self.db.verbose:
            for i, row in enumerate(rows):
                self.logger.debug('row[{i}] {row}')
        return rows

    def commit(self):
        if self.db.cxn.in_transaction:
            self.db.cxn.commit()
        else:
            self.logger.warning('COMMIT requested while not in transaction')

    def handle_warnings(self):
        for warning in self.cursor.fetchwarnings() or []:
            if warning[0] == 'Note' and self.ignore_notes:
                pass
            else:
                self.logger.warning(f"SQL Warning: {warning}")


class Row(dict):

    def __getattr__(self, key):
        return super().__getitem__(key)

    def __setitem__(self, key, value):
        raise KeyError('Row is read-only')

    def __setattr__(self, key, value):
        raise KeyError('Row is read-only')
