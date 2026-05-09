# 数据库操作

## 概述

数据库是 VectorDB 的顶层资源容器，用于组织和隔离表。

---

## 创建数据库

### create_database

```python
db = client.create_database(database_name)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `database_name` | `str` | 是 | 数据库名称 |

**返回值**: `Database` 对象

**示例**:

```python
db = client.create_database('book')
print(f"数据库 '{db.database_name}' 创建成功")
```

**错误码**:

| 错误 | 说明 | 解决方案 |
|------|------|----------|
| `Database Already Exists` | 数据库已存在 | 使用不同的名称 |
| `Illegal Database Name` | 名称不合法 | 使用字母开头的名称 |

---

## 列出数据库

### list_databases

```python
databases = client.list_databases()
```

**返回值**: `list[Database]`

**示例**:

```python
for db in client.list_databases():
    print(f"数据库: {db.database_name}")
```

---

## 获取数据库

### database

获取已存在的数据库对象。

```python
db = client.database(database_name)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `database_name` | `str` | 是 | 数据库名称 |

**返回值**: `Database` 对象

**示例**:

```python
db = client.database('book')
```

**注意**: 如果数据库不存在，将抛出 `ClientError` 异常。

---

## 删除数据库

### drop_database

```python
db.drop_database()
```

**前置条件**: 
- 数据库中所有表必须已删除
- 需要等待表删除完成（约 10 秒）

**示例**:

```python
import time

# 先删除所有表
db.drop_table('my_table')
time.sleep(10)  # 等待表删除完成

# 再删除数据库
db.drop_database()
```

**错误码**:

| 错误 | 说明 | 解决方案 |
|------|------|----------|
| `Database Not Exist` | 数据库不存在 | 检查数据库名称 |
| `Database Not Empty` | 数据库非空 | 先删除所有表 |

---

## 完整示例

```python
import time
import pymochow
from pymochow.configuration import Configuration
from pymochow.auth.bce_credentials import BceCredentials
from pymochow.exception import ClientError, ServerError

# 初始化客户端
config = Configuration(
    credentials=BceCredentials('root', 'your_api_key'),
    endpoint='http://127.0.0.1:8511'
)
client = pymochow.MochowClient(config)

# 创建数据库
try:
    db = client.create_database('test_db')
    print("数据库创建成功")
except ServerError as e:
    print(f"创建失败: {e.message}")

# 列出数据库
print("数据库列表:")
for db_item in client.list_databases():
    print(f"  - {db_item.database_name}")

# 获取数据库
db = client.database('test_db')

# 删除数据库
db.drop_database()
print("数据库已删除")

# 关闭客户端
client.close()
```

---

## Database 对象方法

`Database` 对象提供表操作相关方法：

| 方法 | 说明 |
|------|------|
| `create_table(...)` | 创建表 |
| `describe_table(name)` | 描述表 |
| `list_tables()` | 列出所有表 |
| `table(name)` | 获取表对象 |
| `drop_table(name)` | 删除表 |
| `modify_table(...)` | 修改表参数 |

详细说明请参见 [表操作](./table.md)。

---

**相关文档**:
- [表操作](./table.md)
- [客户端配置](./client.md)
