# @Author: Kehao Wu <wukehao>
# @Date:   2017-04-06T09:50:22+08:00



import psycopg2
from psycopg2 import extensions as _ext

class PgdbError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class Connection:
    """
        初始化
        主要实现四个功能：
        get: fetchone
        query: fetchall
        execute:
        executemany:

        其余功能:
        cursor
        commit
        rollback
        close
    """
    def __init__(self, *args, **kwargs):
        self.connection = None
        self.db_args = args
        self.db_kwargs = kwargs
        self._reconnect()

    def _close(self):
        if self.connection:
            self.connection.close()

    def _cursor(self):
        return self.ensure_connected()

    def cursor(self):
        return self._cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self._close()

    def ensure_connected(self):
        try:
            cursor = self.connection.cursor()
        except Exception as e:
            if isinstance(e, psycopg2.InterfaceError):
                self._reconnect()
            cursor = self.connection.cursor()
        if cursor.connection.get_transaction_status() >= 3:
            self._reconnect()
            cursor = self.connection.cursor()
        return cursor

    def _get_dsn(self):
        may_dbname = self.db_kwargs.get('dbname', -1)
        may_database = self.db_kwargs.get('database', -1)
        if (may_dbname != -1) and may_database:  # 若提供了两个数据库名
            raise ValueError('Name confused: {} or {}?'.format(may_dbname, may_database))
        elif (may_dbname == -1) and (may_database == -1):
            raise ValueError('Should provide the database name to keyword "database" or "dbname"')
        dbname = may_dbname if may_dbname != -1 else may_database
        self.db_kwargs.pop('dbname', None)
        self.db_kwargs.pop('database', None)
        dsn = psycopg2.extensions.make_dsn(*self.db_args, **self.db_kwargs) + " dbname='%s'" % dbname
        return dsn

    def _reconnect(self):
        self._close()
        try:
            self.connection = psycopg2.connect(*self.db_args, **self.db_kwargs)

         # 可能是中文名称数据库：解析中文数据库名可能报错，必须用自行拼接的字符串来初始化数据库连接
         # 见 http://initd.org/psycopg/docs/module.html#psycopg2.connect
        except psycopg2.ProgrammingError:
            dsn = self._get_dsn()
            self.connection = psycopg2.connect(dsn)
        self.autocommit = True

    def query(self, *args, **kwargs):
        cursor = self._cursor()
        try:
            cursor.execute(*args, **kwargs)
            if cursor.description:
                column_names = [column.name for column in cursor.description]
                res = [Row(zip(column_names, row)) for row in cursor.fetchall()]
                cursor.close()
                return res
        except Exception as e:
            if cursor.connection.get_transaction_status() == _ext.TRANSACTION_STATUS_UNKNOWN:
                # 说明网络断开
                self._reconnect()
            else:
                raise e

    def get(self, *args, **kwargs):
        res = self.query(*args, **kwargs)
        if not res:
            return None
        elif len(res) > 1:
            raise PgdbError("Count of result exceed 1")
        else:
            return res[0]

    def execute(self, *args, **kwargs):
        cursor = self._cursor()
        try:
            cursor.execute(*args, **kwargs)
            self.commit()
        except Exception as e:
            if cursor.connection.get_transaction_status() == _ext.TRANSACTION_STATUS_UNKNOWN:
                # 说明网络断开
                self._reconnect()
            else:
                raise e

    def executemany(self, *args, **kwargs):
        cursor = self._cursor()
        try:
            cursor.executemany(*args, **kwargs)
            self.commit()
        except Exception as e:
            if cursor.connection.get_transaction_status() == _ext.TRANSACTION_STATUS_UNKNOWN:
                # 说明网络断开
                self._reconnect()
            else:
                raise e



class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
