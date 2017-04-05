import psycopg2

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

    def _reconnect(self):
        self._close()
        self.connection = psycopg2.connect(*self.db_args, **self.db_kwargs)
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
            raise e

    def executemany(self, *args, **kwargs):
        cursor = self._cursor()
        try:
            cursor.executemany(*args, **kwargs)
            self.commit()
        except Exception as e:
            raise e



class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
