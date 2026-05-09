# 表操作

## 概述

表是 VectorDB 中存储数据的基本单元，包含字段定义和索引配置。

---

## 创建表

### create_table

```python
db.create_table(
    table_name,
    replication,
    partition,
    schema
)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `table_name` | `str` | 是 | 表名称 |
| `replication` | `int` | 是 | 副本数，建议设为 3 |
| `partition` | `Partition` | 是 | 分区配置 |
| `schema` | `Schema` | 是 | 表结构定义 |

**示例**:

```python
from pymochow.model.schema import Schema, Field, VectorIndex, HNSWParams
from pymochow.model.enum import FieldType, IndexType, MetricType
from pymochow.model.table import Partition

# 定义字段
fields = [
    Field("id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("bookName", FieldType.STRING, not_null=True),
    Field("author", FieldType.STRING),
    Field("page", FieldType.UINT32),
    Field("content", FieldType.TEXT),
    Field("vector", FieldType.FLOAT_VECTOR, not_null=True, dimension=768),
]

# 定义索引
indexes = [
    VectorIndex(
        index_name="vector_idx",
        index_type=IndexType.HNSW,
        field="vector",
        metric_type=MetricType.L2,
        params=HNSWParams(m=32, efconstruction=200)
    )
]

# 创建表
db.create_table(
    table_name='book_segments',
    replication=3,
    partition=Partition(partition_num=1),
    schema=Schema(fields=fields, indexes=indexes)
)
```

---

## Field 字段定义

### 构造参数

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | `str` | 是 | 字段名称 |
| `field_type` | `FieldType` | 是 | 字段类型 |
| `primary_key` | `bool` | 否 | 是否为主键 |
| `partition_key` | `bool` | 否 | 是否为分区键 |
| `not_null` | `bool` | 否 | 是否非空 |
| `dimension` | `int` | 向量字段必填 | 向量维度 |
| `element_type` | `ElementType` | ARRAY 必填 | 数组元素类型 |
| `auto_increment` | `bool` | 否 | 是否自增（不推荐） |

### 字段类型

| FieldType | 说明 | 示例 |
|-----------|------|------|
| `STRING` | 字符串 | `"hello"` |
| `INT64` | 64位整数 | `42` |
| `UINT32` | 32位无符号整数 | `100` |
| `DOUBLE` | 双精度浮点数 | `3.14` |
| `BOOL` | 布尔值 | `True` |
| `TEXT` | 长文本 | 大段文字 |
| `FLOAT_VECTOR` | 稠密向量 | `[0.1, 0.2, ...]` |
| `BINARY_VECTOR` | 二进制向量 | 二进制数据 |
| `SPARSE_FLOAT_VECTOR` | 稀疏向量 | `{1: 0.5, 100: 0.3}` |
| `JSON` | JSON 对象 | `{"key": "value"}` |
| `ARRAY` | 数组 | `["a", "b"]` |

### 示例

```python
from pymochow.model.enum import ElementType

# ARRAY 类型字段
Field("tags", FieldType.ARRAY, element_type=ElementType.STRING)

# JSON 类型字段
Field("metadata", FieldType.JSON)

# 向量字段（768维）
Field("embedding", FieldType.FLOAT_VECTOR, dimension=768, not_null=True)
```

---

## Partition 分区配置

| 参数 | 类型 | 说明 |
|------|------|------|
| `partition_num` | `int` | 分区数量 |

### 分区数建议

| 数据规模 | 建议分区数 |
|----------|------------|
| 1-10万行 | 1 |
| 10万-100万行 | 4 |
| 100万-1000万行 | 16 |
| 1000万-5000万行 | 64 |

---

## 描述表

### describe_table

```python
table_info = db.describe_table(table_name)
```

**返回值**: `Table` 对象，包含表的详细信息

**示例**:

```python
table = db.describe_table('book_segments')
print(f"表名: {table.table_name}")
print(f"状态: {table.state}")
print(f"分区数: {table.partition.partition_num}")
```

---

## 等待表就绪

表创建后需要等待状态变为 `NORMAL`：

```python
import time
from pymochow.model.enum import TableState

while True:
    time.sleep(2)
    table = db.describe_table('book_segments')
    if table.state == TableState.NORMAL:
        print("表已就绪")
        break
```

---

## 修改表

### modify_table

```python
db.modify_table(table_name, **kwargs)
```

**参数**:

| 参数 | 类型 | 说明 |
|------|------|------|
| `table_name` | `str` | 表名 |
| `datanode_memory_reserved_in_gb` | `float` | 预留内存（GB） |

**示例**:

```python
db.modify_table('book_segments', datanode_memory_reserved_in_gb=0.1)
```

---

## 添加字段

### add_fields

向已有表添加新字段（不支持添加向量字段）。

```python
table = db.table('book_segments')
fields = [
    Field("publisher", FieldType.STRING),
    Field("synopsis", FieldType.STRING),
]
table.add_fields(schema=Schema(fields=fields))
```

---

## 表别名

### alias / unalias

为表创建别名：

```python
table = db.table('book_segments')

# 创建别名
table.alias('book_alias')

# 删除别名
table.unalias('book_alias')
```

---

## 获取表统计信息

### stats

```python
table = db.table('book_segments')
stats = table.stats()
print(f"行数: {stats.row_count}")
```

---

## 删除表

### drop_table

```python
db.drop_table('book_segments')
```

> **注意**: 删除操作是异步的，需等待约 10 秒后才能删除数据库。

---

## 完整示例

```python
import time
from pymochow.model.schema import Schema, Field, VectorIndex, HNSWParams, SecondaryIndex
from pymochow.model.enum import FieldType, IndexType, MetricType, TableState
from pymochow.model.table import Partition

# 定义字段
fields = [
    Field("id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("bookName", FieldType.STRING, not_null=True),
    Field("author", FieldType.STRING),
    Field("vector", FieldType.FLOAT_VECTOR, not_null=True, dimension=768),
]

# 定义索引
indexes = [
    VectorIndex("vector_idx", IndexType.HNSW, "vector", MetricType.L2, HNSWParams(m=32, efconstruction=200)),
    SecondaryIndex("book_name_idx", "bookName"),
]

# 创建表
db.create_table(
    table_name='books',
    replication=3,
    partition=Partition(partition_num=4),
    schema=Schema(fields=fields, indexes=indexes)
)

# 等待就绪
while db.describe_table('books').state != TableState.NORMAL:
    time.sleep(2)

# 获取表对象
table = db.table('books')

# 添加字段
table.add_fields(schema=Schema(fields=[Field("summary", FieldType.TEXT)]))

# 创建别名
table.alias('books_v1')

# 删除
table.unalias('books_v1')
db.drop_table('books')
```

---

**相关文档**:
- [数据操作](./data-operations.md)
- [索引操作](./index-operations.md)
- [数据模型](../core-concepts/data-model.md)
