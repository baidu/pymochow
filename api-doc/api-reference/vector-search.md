# 向量检索

## 概述

VectorDB 提供多种向量检索方式：TopK 检索、范围检索、批量检索和多向量检索。

---

## TopK 检索

### VectorTopkSearchRequest

返回与查询向量最相似的 Top K 个结果。

```python
from pymochow.model.table import VectorTopkSearchRequest, FloatVector, VectorSearchConfig

request = VectorTopkSearchRequest(
    vector_field="vector",
    vector=FloatVector([1, 0.21, 0.213, 0]),
    limit=10,
    filter="bookName='三国演义'",
    config=VectorSearchConfig(ef=200, pruning=True)
)

result = table.vector_search(request=request)
for row in result.rows:
    print(f"ID: {row.id}, Distance: {row.distance}")
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `vector_field` | `str` | 是 | 向量字段名 |
| `vector` | `FloatVector` | 是 | 查询向量 |
| `limit` | `int` | 是 | 返回数量 |
| `filter` | `str` | 否 | 过滤条件 |
| `config` | `VectorSearchConfig` | 否 | 检索配置 |

---

## 范围检索

### VectorRangeSearchRequest

返回距离在指定范围内的结果。

```python
from pymochow.model.table import VectorRangeSearchRequest, FloatVector, VectorSearchConfig

request = VectorRangeSearchRequest(
    vector_field="vector",
    vector=FloatVector([1, 0.21, 0.213, 0]),
    distance_range=(0, 20),  # 距离范围
    limit=10,
    filter="bookName='三国演义'",
    config=VectorSearchConfig(ef=200, pruning=True)
)

result = table.vector_search(request=request)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `vector_field` | `str` | 是 | 向量字段名 |
| `vector` | `FloatVector` | 是 | 查询向量 |
| `distance_range` | `tuple` | 是 | 距离范围 `(min, max)` |
| `limit` | `int` | 否 | 返回数量限制 |
| `filter` | `str` | 否 | 过滤条件 |
| `config` | `VectorSearchConfig` | 否 | 检索配置 |

---

## 批量检索

### VectorBatchSearchRequest

一次请求执行多个向量的检索。

```python
from pymochow.model.table import VectorBatchSearchRequest, FloatVector, VectorSearchConfig

request = VectorBatchSearchRequest(
    vector_field="vector",
    vectors=[
        FloatVector([1, 0.21, 0.213, 0]),
        FloatVector([1, 0.32, 0.513, 0]),
    ],
    limit=10,
    filter="bookName='三国演义'",
    config=VectorSearchConfig(ef=200, pruning=True)
)

result = table.vector_search(request=request)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `vector_field` | `str` | 是 | 向量字段名 |
| `vectors` | `list[FloatVector]` | 是 | 查询向量列表 |
| `limit` | `int` | 是 | 每个向量返回数量 |
| `filter` | `str` | 否 | 过滤条件 |
| `config` | `VectorSearchConfig` | 否 | 检索配置 |

---

## 多向量检索

### MultiVectorSearchRequest

在多个向量字段上搜索并融合结果。

```python
from pymochow.model.table import VectorTopkSearchRequest, MultiVectorSearchRequest, FloatVector, VectorSearchConfig
from pymochow.model.schema import RRFRank, WeightedRank

# 子搜索请求
requests = [
    VectorTopkSearchRequest(
        vector_field="vector",
        vector=FloatVector([1, 0.21, 0.213, 0]),
        limit=10,
        config=VectorSearchConfig(ef=200)
    ),
    VectorTopkSearchRequest(
        vector_field="vector",
        vector=FloatVector([1, 0.21, 0.213, 0]),
        limit=10,
        config=VectorSearchConfig(ef=200)
    )
]

# 多向量检索
request = MultiVectorSearchRequest(
    requests=requests,
    ranking=RRFRank(60),  # 或 WeightedRank([1.0, 1.0])
    limit=10,
    filter="bookName='三国演义'"
)

result = table.vector_search(request=request, projections=['id'])
```

### 排名策略

| 策略 | 说明 | 用法 |
|------|------|------|
| `RRFRank(k)` | 倒数排名融合 | `RRFRank(60)` |
| `WeightedRank(weights)` | 加权融合 | `WeightedRank([0.5, 0.5])` |

---

## VectorSearchConfig

检索配置参数。

### HNSW 配置

```python
config = VectorSearchConfig(
    ef=200,       # 检索广度
    pruning=True  # 是否启用剪枝
)
```

### HNSWPQ/HNSWSQ 配置

```python
config = VectorSearchConfig(ef=200)
```

### PUCK 配置

```python
config = VectorSearchConfig(search_coarse_count=5)
```

### DISKANN 配置

```python
config = VectorSearchConfig(
    w=1,          # beam search 宽度
    search_l=100  # 搜索范围
)
```

### 配置参数速查

| 索引类型 | 关键参数 | 说明 |
|----------|----------|------|
| HNSW | `ef` | 检索广度，越大越精确 |
| HNSW | `pruning` | 是否剪枝优化 |
| HNSWPQ | `ef` | 检索广度 |
| HNSWSQ | `ef` | 检索广度 |
| PUCK | `search_coarse_count` | 粗搜索数量 |
| DISKANN | `w`, `search_l` | beam宽度和搜索范围 |

---

## 向量类型

### FloatVector

稠密向量（标准浮点向量）。

```python
from pymochow.model.table import FloatVector

vector = FloatVector([0.1, 0.2, 0.3, 0.4])
```

### BinaryVector

二进制向量。

```python
from pymochow.model.table import BinaryVector

# 从二进制列表创建
vector = BinaryVector.from_binary_list([1, 0, 1, 1, 0, 0, 1, 0])
```

### SparseFloatVector

稀疏向量。

```python
from pymochow.model.table import SparseFloatVector

# 从字典创建 {索引: 值}
vector = SparseFloatVector.from_dict({1: 0.56, 100: 0.23, 10000: 0.54})
```

---

## 返回结果

检索结果包含匹配的行列表：

```python
result = table.vector_search(request=request)

for row in result.rows:
    print(f"ID: {row.id}")
    print(f"Distance: {row.distance}")
    print(f"Score: {row.score}")
```

### 指定返回字段

使用 `projections` 参数指定返回字段：

```python
result = table.vector_search(
    request=request,
    projections=['id', 'title', 'author']
)
```

> **建议**: 不要在 `projections` 中包含向量字段，以提高性能。

---

## 过滤条件语法

使用 SQL-like 语法编写过滤条件：

| 操作 | 语法 | 示例 |
|------|------|------|
| 等于 | `=` | `bookName='三国演义'` |
| 不等于 | `!=` | `status!='deleted'` |
| 大于 | `>` | `page>100` |
| 小于 | `<` | `page<200` |
| 大于等于 | `>=` | `price>=9.99` |
| 小于等于 | `<=` | `price<=99.99` |
| AND | `AND` | `a='x' AND b>10` |
| OR | `OR` | `a='x' OR a='y'` |
| IN | `IN` | `status IN ('active', 'pending')` |

**示例**:

```python
# 单条件
filter = "bookName='三国演义'"

# 多条件
filter = "bookName='三国演义' AND page>50"

# 复杂条件
filter = "(author='罗贯中' OR author='吴承恩') AND page>=10"
```

---

## 完整示例

```python
import time
from pymochow.model.table import (
    VectorTopkSearchRequest,
    VectorRangeSearchRequest,
    VectorBatchSearchRequest,
    MultiVectorSearchRequest,
    FloatVector,
    VectorSearchConfig
)
from pymochow.model.schema import RRFRank
from pymochow.model.enum import IndexState

# 获取表
table = db.table('book_segments')

# 重建索引
table.rebuild_index("vector_idx")
while table.describe_index("vector_idx").state != IndexState.NORMAL:
    time.sleep(2)

# TopK 检索
topk_request = VectorTopkSearchRequest(
    vector_field="vector",
    vector=FloatVector([1, 0.21, 0.213, 0]),
    limit=10,
    filter="bookName='三国演义'",
    config=VectorSearchConfig(ef=200, pruning=True)
)
result = table.vector_search(request=topk_request)
print("TopK 结果:", result)

# 范围检索
range_request = VectorRangeSearchRequest(
    vector_field="vector",
    vector=FloatVector([1, 0.21, 0.213, 0]),
    distance_range=(0, 20),
    limit=10,
    config=VectorSearchConfig(ef=200)
)
result = table.vector_search(request=range_request)
print("范围检索结果:", result)

# 批量检索
batch_request = VectorBatchSearchRequest(
    vector_field="vector",
    vectors=[
        FloatVector([1, 0.21, 0.213, 0]),
        FloatVector([2, 0.22, 0.223, 0])
    ],
    limit=5,
    config=VectorSearchConfig(ef=200)
)
result = table.vector_search(request=batch_request)
print("批量检索结果:", result)

# 多向量检索
multi_request = MultiVectorSearchRequest(
    requests=[topk_request, topk_request],
    ranking=RRFRank(60),
    limit=10
)
result = table.vector_search(request=multi_request, projections=['id'])
print("多向量检索结果:", result)
```

---

**相关文档**:
- [混合检索](../advanced/hybrid-search.md)
- [索引操作](./index-operations.md)
- [索引类型](../core-concepts/index-types.md)
