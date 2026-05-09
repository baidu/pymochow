# 搜索迭代器

## 概述

搜索迭代器用于分批获取大量检索结果，避免单次请求返回过多数据。

---

## search_iterator

### 基本用法

```python
iterator = table.search_iterator(
    request=request,
    batch_size=1000,
    total_size=10000
)

while True:
    rows = iterator.next()
    if not rows:
        break
    for row in rows:
        print(f"ID: {row.id}")

iterator.close()
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `request` | 检索请求 | 是 | TopK 或 MultiVector 请求 |
| `batch_size` | `int` | 是 | 每批返回数量 |
| `total_size` | `int` | 是 | 总共获取数量 |

---

## TopK 迭代器

```python
from pymochow.model.table import VectorTopkSearchRequest, FloatVector, VectorSearchConfig

# 创建检索请求
request = VectorTopkSearchRequest(
    vector_field="vector",
    vector=FloatVector([1, 0.21, 0.213, 0]),
    limit=1000,
    config=VectorSearchConfig(ef=2000)
)

# 创建迭代器
iterator = table.search_iterator(
    request=request,
    batch_size=1000,
    total_size=10000
)

# 分批获取结果
all_results = []
while True:
    rows = iterator.next()
    if not rows:
        break
    all_results.extend(rows)
    print(f"已获取 {len(all_results)} 条结果")

# 关闭迭代器
iterator.close()
```

---

## 多向量迭代器

```python
from pymochow.model.table import VectorTopkSearchRequest, MultiVectorSearchRequest, FloatVector, VectorSearchConfig
from pymochow.model.schema import WeightedRank

# 创建子检索请求
config = VectorSearchConfig(ef=2000)
sub_requests = [
    VectorTopkSearchRequest("vector", FloatVector([1, 0.21, 0.213, 0]), 1000, config=config),
    VectorTopkSearchRequest("vector", FloatVector([1, 0.21, 0.213, 0]), 1000, config=config),
]

# 创建多向量检索请求
request = MultiVectorSearchRequest(
    requests=sub_requests,
    ranking=WeightedRank([1.0, 1.0]),
    limit=1000
)

# 创建迭代器
iterator = table.search_iterator(
    request=request,
    batch_size=1000,
    total_size=10000
)

# 分批获取结果
while True:
    rows = iterator.next()
    if not rows:
        break
    for row in rows:
        print(f"ID: {row.id}, Score: {row.score}")

iterator.close()
```

---

## 注意事项

> **重要**: 搜索迭代器目前仅支持 HNSW 和 HNSWPQ 索引类型。

```python
from pymochow.model.enum import IndexType

# 检查索引类型
if index_type not in [IndexType.HNSW, IndexType.HNSWPQ]:
    print("搜索迭代器仅支持 HNSW 和 HNSWPQ 索引")
```

---

## 完整示例

```python
import time
from pymochow.model.table import (
    VectorTopkSearchRequest,
    MultiVectorSearchRequest,
    FloatVector,
    VectorSearchConfig
)
from pymochow.model.schema import RRFRank
from pymochow.model.enum import IndexState

# 获取表
table = db.table('book_segments')

# 确保索引就绪
while table.describe_index("vector_idx").state != IndexState.NORMAL:
    time.sleep(2)

# TopK 迭代器示例
print("=== TopK 迭代器 ===")
request = VectorTopkSearchRequest(
    vector_field="vector",
    vector=FloatVector([1, 0.21, 0.213, 0]),
    limit=1000,
    config=VectorSearchConfig(ef=2000)
)

iterator = table.search_iterator(request=request, batch_size=100, total_size=500)
count = 0
while True:
    rows = iterator.next()
    if not rows:
        break
    count += len(rows)
    print(f"批次获取 {len(rows)} 条, 累计 {count} 条")
iterator.close()

# 多向量迭代器示例
print("\n=== 多向量迭代器 ===")
config = VectorSearchConfig(ef=2000)
sub_requests = [
    VectorTopkSearchRequest("vector", FloatVector([1, 0.21, 0.213, 0]), 1000, config=config),
    VectorTopkSearchRequest("vector", FloatVector([2, 0.31, 0.313, 0]), 1000, config=config),
]

multi_request = MultiVectorSearchRequest(
    requests=sub_requests,
    ranking=RRFRank(60),
    limit=1000
)

iterator2 = table.search_iterator(request=multi_request, batch_size=100, total_size=500)
count = 0
while True:
    rows = iterator2.next()
    if not rows:
        break
    count += len(rows)
    print(f"批次获取 {len(rows)} 条, 累计 {count} 条")
iterator2.close()
```

---

## 使用场景

| 场景 | 说明 |
|------|------|
| 大量结果导出 | 将检索结果导出到文件 |
| 流式处理 | 边获取边处理，减少内存占用 |
| 分页展示 | 前端分页展示大量结果 |

---

**相关文档**:
- [向量检索](../api-reference/vector-search.md)
- [索引类型](../core-concepts/index-types.md)
