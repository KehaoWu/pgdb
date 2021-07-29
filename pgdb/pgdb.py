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
        self.max_retry_count = 5
        self.retry_count = 0

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

    def _reconnect(self):
        self._close()
        self.connection = psycopg2.connect(*self.db_args, **self.db_kwargs)
        self.autocommit = True

    def can_retry(self):
        self.retry_count = self.retry_count + 1
        can_retry = self.retry_count <= self.max_retry_count
        if not can_retry:
            raise PgdbError("Retry count {} exceed max retry count {}".format(self.retry_count, self.max_retry_count))
        return can_retry

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
                if self.can_retry():
                    return self.query(*args, **kwargs)
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
                if self.can_retry():
                    self.execute(*args, **kwargs)
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
                if self.can_retry():
                    self.executemany(*args, **kwargs)
            else:
                raise e



class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
