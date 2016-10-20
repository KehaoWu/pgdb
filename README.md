# PostgreSQL 包装

## 安装
```
sudo pip install pgdb
# sudo python3 -m pip install pgdb
```

## 使用方法

```
from pgdb import Connection

## 链接PostgreSQL
connection = Connection(user='postgres', database='main', host='localhost')

## 执行普通查询，查询结果会根据字段名进行包装编程字典，不再是元组
connection.query("select * from main")

## 执行单条查询，结果与query类似
connection.get("select * from main limit 1")

## 执行普通操作
connection.execute("update main set status = 0")

## 执行普通多条操作
connection.executemany("insert into main values %(status)s", [{'status': 1}])

# 其他操作

cursor = connection.cursor()
connection.commit()
connection.rollback()
connection.close()
```

欢迎大家前来贡献代码

# For development
```
python setup.py sdist

## edit ~/.pypirc
### [pypi]
### repository = https://pypi.python.org/pypi
### username =  
### password =  

twine register dist/pgdb-0.0.1.tar.gz
twine upload dist/pgdb-0.0.1.tar.gz
```
