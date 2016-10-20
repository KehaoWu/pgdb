import psycopg2

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
        self.connection = psycopg2.connect(*args, **kwargs)
        self.autocommit = True

    def _close(self):
        self.connection.close()

    def _cursor(self):
        return self.connection.cursor()

    def cursor(self):
        return self._cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self._close()

    def query(self, *args, **kwargs):
        cursor = self._cursor()
        try:
            cursor.execute(*args, **kwargs)
            if cursor.description:
                column_names = [column.name for column in cursor.description]
                res = [Row(zip(column_names, row)) for row in cursor.fetchall()]
                cursor.close()
                return res
        except:
            cursor.close()
            raise

    def get(self, *args, **kwargs):
        res = self.query(*args, **kwargs)
        if not res:
            return None
        elif len(res) > 1:
            raise
        else:
            return res[0]

    def execute(self, *args, **kwargs):
        cursor = self._cursor()
        try:
            cursor.execute(*args, **kwargs)
            self.commit()
        except:
            raise

        finally:
            cursor.close()

    def executemany(self, *args, **kwargs):
        cursor = self._cursor()
        try:
            cursor.executemany(*args, **kwargs)
            self.commit()
        except:
            raise

        finally:
            cursor.close()



class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
