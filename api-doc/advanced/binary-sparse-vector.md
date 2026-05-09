# Binary/Sparse 向量

## 概述

除了标准的稠密向量（FloatVector），VectorDB 还支持二进制向量（BinaryVector）和稀疏向量（SparseFloatVector）。

---

## 二进制向量

### BinaryVector

二进制向量使用 0/1 表示，适合特定的哈希场景。

### 创建表

```python
from pymochow.model.schema import Schema, Field
from pymochow.model.enum import FieldType
from pymochow.model.table import Partition

fields = [
    Field("id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("vector", FieldType.BINARY_VECTOR, not_null=True, dimension=128),
]

db.create_table(
    table_name="binary_table",
    replication=3,
    partition=Partition(partition_num=1),
    schema=Schema(fields=fields)
)
```

### 创建向量

```python
from pymochow.model.table import BinaryVector

# 从二进制列表创建（0/1 列表）
binary_list = [1, 0, 1, 1, 0, 0, 1, 0] * 16  # 128 bits
vector = BinaryVector.from_binary_list(binary_list)
```

### 插入数据

```python
from pymochow.model.table import Row, BinaryVector

def num_to_binary_list(num, dimension):
    """将数字转换为二进制列表"""
    binary_str = bin(num)[2:]
    binary_list = [int(bit) for bit in binary_str]
    # 补齐或截断到指定维度
    if len(binary_list) > dimension:
        binary_list = binary_list[:dimension]
    elif len(binary_list) < dimension:
        binary_list = [0] * (dimension - len(binary_list)) + binary_list
    return binary_list

table = db.table("binary_table")
rows = []
for num in range(50):
    vec = BinaryVector.from_binary_list(num_to_binary_list(num, 128))
    rows.append(Row(id=str(num), vector=vec))

table.upsert(rows=rows)
```

### 检索

```python
from pymochow.model.table import VectorTopkSearchRequest, BinaryVector

target = BinaryVector.from_binary_list(num_to_binary_list(123, 128))
request = VectorTopkSearchRequest(
    vector_field="vector",
    vector=target,
    limit=10
)

result = table.vector_search(request=request)
for row in result.rows:
    print(f"ID: {row.id}, Distance: {row.distance}")
```

---

## 稀疏向量

### SparseFloatVector

稀疏向量只存储非零元素，适合高维稀疏表示（如 TF-IDF、BM25）。

### 创建表和索引

```python
from pymochow.model.schema import Schema, Field, VectorIndex
from pymochow.model.enum import FieldType, IndexType, MetricType
from pymochow.model.table import Partition

fields = [
    Field("id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("vector", FieldType.SPARSE_FLOAT_VECTOR, not_null=True),
]

indexes = [
    VectorIndex(
        index_name="sparse_vector_idx",
        index_type=IndexType.SPARSE_OPTIMIZED_FLAT,
        field="vector",
        metric_type=MetricType.IP
    )
]

db.create_table(
    table_name="sparse_table",
    replication=3,
    partition=Partition(partition_num=1),
    schema=Schema(fields=fields, indexes=indexes)
)
```

### 创建向量

```python
from pymochow.model.table import SparseFloatVector

# 从字典创建 {索引: 值}
vector = SparseFloatVector.from_dict({
    1: 0.56465,
    100: 0.2366456,
    10000: 0.543111
})
```

### 插入数据

```python
from pymochow.model.table import Row, SparseFloatVector

table = db.table("sparse_table")
rows = []
for num in range(50):
    vec = SparseFloatVector.from_dict({
        1: 0.56465,
        100: 0.2366456,
        10000: 0.543111
    })
    rows.append(Row(id=str(num), vector=vec))

table.upsert(rows=rows)
```

### 检索

```python
from pymochow.model.table import VectorTopkSearchRequest, SparseFloatVector

target = SparseFloatVector.from_dict({
    1: 0.56465,
    100: 0.2366456,
    10000: 0.543111
})

request = VectorTopkSearchRequest(
    vector_field="vector",
    vector=target,
    limit=10
)

result = table.vector_search(request=request)
for row in result.rows:
    print(f"ID: {row.id}, Score: {row.score}")
```

---

## 完整示例

### 二进制向量示例

```python
import time
from pymochow.model.schema import Schema, Field
from pymochow.model.enum import FieldType, TableState
from pymochow.model.table import Partition, Row, BinaryVector, VectorTopkSearchRequest

def num_to_binary_list(num, dimension):
    binary_str = bin(num)[2:]
    binary_list = [int(bit) for bit in binary_str]
    if len(binary_list) > dimension:
        binary_list = binary_list[:dimension]
    elif len(binary_list) < dimension:
        binary_list = [0] * (dimension - len(binary_list)) + binary_list
    return binary_list

# 1. 创建表
fields = [
    Field("id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("vector", FieldType.BINARY_VECTOR, not_null=True, dimension=128),
]
db.create_table("binary_demo", replication=3, partition=Partition(1), schema=Schema(fields))

while db.describe_table("binary_demo").state != TableState.NORMAL:
    time.sleep(2)

# 2. 插入数据
table = db.table("binary_demo")
rows = [
    Row(id=str(i), vector=BinaryVector.from_binary_list(num_to_binary_list(i, 128)))
    for i in range(50)
]
table.upsert(rows=rows)

# 3. 检索
target = BinaryVector.from_binary_list(num_to_binary_list(25, 128))
result = table.vector_search(VectorTopkSearchRequest("vector", target, 10))
print("二进制向量检索结果:", result)
```

### 稀疏向量示例

```python
import time
from pymochow.model.schema import Schema, Field, VectorIndex
from pymochow.model.enum import FieldType, IndexType, MetricType, TableState
from pymochow.model.table import Partition, Row, SparseFloatVector, VectorTopkSearchRequest

# 1. 创建表
fields = [
    Field("id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("vector", FieldType.SPARSE_FLOAT_VECTOR, not_null=True),
]
indexes = [
    VectorIndex("sparse_idx", IndexType.SPARSE_OPTIMIZED_FLAT, "vector", MetricType.IP)
]
db.create_table("sparse_demo", replication=3, partition=Partition(1), 
                schema=Schema(fields, indexes))

while db.describe_table("sparse_demo").state != TableState.NORMAL:
    time.sleep(2)

# 2. 插入数据
table = db.table("sparse_demo")
rows = [
    Row(id=str(i), vector=SparseFloatVector.from_dict({1: 0.5, 100: 0.3, 10000: 0.2}))
    for i in range(50)
]
table.upsert(rows=rows)

# 3. 检索
target = SparseFloatVector.from_dict({1: 0.5, 100: 0.3, 10000: 0.2})
result = table.vector_search(VectorTopkSearchRequest("vector", target, 10))
print("稀疏向量检索结果:", result)
```

---

## 使用场景

| 向量类型 | 适用场景 |
|----------|----------|
| 稠密向量 | 语义检索、图像检索 |
| 二进制向量 | 局部敏感哈希、快速过滤 |
| 稀疏向量 | TF-IDF、BM25、关键词检索 |

---

**相关文档**:
- [向量检索](../api-reference/vector-search.md)
- [数据模型](../core-concepts/data-model.md)
