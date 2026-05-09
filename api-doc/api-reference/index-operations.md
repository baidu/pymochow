# 索引操作

## 概述

索引是加速检索的关键组件。VectorDB 支持向量索引、二级索引、过滤索引和倒排索引。

---

## 创建索引

### create_indexes

在表上创建索引。

```python
table.create_indexes(indexes)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `indexes` | `list` | 是 | 索引列表 |

---

## 向量索引

### VectorIndex

```python
from pymochow.model.schema import VectorIndex, HNSWParams
from pymochow.model.enum import IndexType, MetricType

index = VectorIndex(
    index_name="vector_idx",
    index_type=IndexType.HNSW,
    field="vector",
    metric_type=MetricType.L2,
    params=HNSWParams(m=32, efconstruction=200),
    auto_build=True
)
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `index_name` | `str` | 是 | 索引名称 |
| `index_type` | `IndexType` | 是 | 索引类型 |
| `field` | `str` | 是 | 向量字段名 |
| `metric_type` | `MetricType` | 是 | 度量类型 |
| `params` | 索引参数 | 是 | 索引构建参数 |
| `auto_build` | `bool` | 否 | 是否自动构建 |

### 索引类型和参数

#### HNSW

```python
from pymochow.model.schema import HNSWParams

params = HNSWParams(m=32, efconstruction=200)
```

| 参数 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| `m` | 16 | 4-64 | 每个节点的邻居数 |
| `efconstruction` | 200 | 100-400 | 构建时搜索广度 |

#### HNSWPQ

```python
from pymochow.model.schema import HNSWPQParams

params = HNSWPQParams(m=16, efconstruction=200, NSQ=4, samplerate=1.0)
```

| 参数 | 说明 |
|------|------|
| `NSQ` | 子向量数量 |
| `samplerate` | 采样率 |

#### HNSWSQ

```python
from pymochow.model.schema import HNSWSQParams

params = HNSWSQParams(m=16, efconstruction=200, qtBits=8)
```

| 参数 | 说明 |
|------|------|
| `qtBits` | 量化位数，通常为 8 |

#### PUCK

```python
from pymochow.model.schema import PUCKParams

params = PUCKParams(coarseClusterCount=5, fineClusterCount=5)
```

#### DISKANN

```python
from pymochow.model.schema import DISKANNParams

params = DISKANNParams(NSQ=4, R=64, L=100)
```

| 参数 | 说明 |
|------|------|
| `NSQ` | 子空间数量 |
| `R` | 候选列表大小 |
| `L` | 图层大小 |

### 度量类型

| MetricType | 说明 |
|------------|------|
| `L2` | 欧氏距离 |
| `IP` | 内积 |
| `COSINE` | 余弦相似度 |

---

## 二级索引

### SecondaryIndex

用于加速标量字段的查询。

```python
from pymochow.model.schema import SecondaryIndex

index = SecondaryIndex(
    index_name="book_name_idx",
    field="bookName"
)
```

---

## 过滤索引

### FilteringIndex

用于加速过滤条件的执行。

```python
from pymochow.model.schema import FilteringIndex

index = FilteringIndex(
    index_name="filter_idx",
    fields=["bookName", "author"]
)
```

> **重要**: 过滤条件和投影中使用的字段必须建立过滤索引。

---

## 倒排索引

### InvertedIndex

用于全文检索和 BM25 搜索。

```python
from pymochow.model.schema import InvertedIndex, InvertedIndexParams
from pymochow.model.enum import (
    InvertedIndexAnalyzer,
    InvertedIndexParseMode,
    InvertedIndexFieldAttribute
)

index = InvertedIndex(
    index_name="content_inverted_idx",
    fields=["content"],
    params=InvertedIndexParams(
        analyzer=InvertedIndexAnalyzer.CHINESE_ANALYZER,
        parse_mode=InvertedIndexParseMode.COARSE_MODE,
        case_sensitive=True
    ),
    field_attributes=[InvertedIndexFieldAttribute.ANALYZED]
)
```

**分词器**:

| Analyzer | 说明 |
|----------|------|
| `CHINESE_ANALYZER` | 中文分词器 |
| `STANDARD_ANALYZER` | 标准分词器 |

---

## 描述索引

### describe_index

获取索引详细信息。

```python
index = table.describe_index("vector_idx")
print(f"索引名: {index.index_name}")
print(f"状态: {index.state}")
```

### 索引状态

| IndexState | 说明 |
|------------|------|
| `INVALID` | 索引无效或刚创建 |
| `BUILDING` | 正在构建 |
| `NORMAL` | 索引可用 |

---

## 修改索引

### modify_index

修改索引配置，如自动重建策略。

```python
from pymochow.model.schema import AutoBuildTiming

table.modify_index(
    index_name="vector_idx",
    auto_build=True,
    auto_build_index_policy=AutoBuildTiming("2024-01-01 00:00:00")
)
```

**参数**:

| 参数 | 类型 | 说明 |
|------|------|------|
| `index_name` | `str` | 索引名称 |
| `auto_build` | `bool` | 是否自动构建 |
| `auto_build_index_policy` | 策略对象 | 自动构建策略 |

### 自动重建策略

| 策略 | 用法 | 场景 |
|------|------|------|
| `AutoBuildTiming` | 定时触发 | 每天定时导入 |
| `AutoBuildPeriodical` | 周期性触发 | 低峰期重建 |
| `AutoBuildRowCountIncrement` | 增量行触发 | 24小时持续写入 |

---

## 重建索引

### rebuild_index

手动触发索引重建。

```python
table.rebuild_index("vector_idx")
```

### 等待重建完成

```python
import time
from pymochow.model.enum import IndexState

table.rebuild_index("vector_idx")

while True:
    time.sleep(2)
    index = table.describe_index("vector_idx")
    if index.state == IndexState.NORMAL:
        print("索引重建完成")
        break
```

### 何时需要重建

- 大量数据导入后
- 删除大量数据后
- 索引性能下降时

---

## 删除索引

### drop_index

删除向量索引。

```python
table.drop_index("vector_idx")
```

### 等待删除完成

```python
from pymochow.exception import ServerError
from pymochow.model.enum import ServerErrCode

table.drop_index("vector_idx")

while True:
    time.sleep(2)
    try:
        index = table.describe_index("vector_idx")
    except ServerError as e:
        if e.code == ServerErrCode.INDEX_NOT_EXIST:
            print("索引已删除")
            break
```

> **注意**: 倒排索引不支持删除。

---

## 完整示例

```python
import time
from pymochow.model.schema import (
    VectorIndex, SecondaryIndex, FilteringIndex, InvertedIndex,
    HNSWParams, InvertedIndexParams, AutoBuildTiming
)
from pymochow.model.enum import (
    IndexType, MetricType, IndexState,
    InvertedIndexAnalyzer, InvertedIndexParseMode, InvertedIndexFieldAttribute
)
from pymochow.exception import ServerError
from pymochow.model.enum import ServerErrCode

# 获取表
table = db.table('book_segments')

# 删除旧索引
table.drop_index("vector_idx")
while True:
    time.sleep(2)
    try:
        table.describe_index("vector_idx")
    except ServerError as e:
        if e.code == ServerErrCode.INDEX_NOT_EXIST:
            break

# 创建新索引
indexes = [
    VectorIndex(
        index_name="vector_idx",
        index_type=IndexType.HNSW,
        field="vector",
        metric_type=MetricType.L2,
        params=HNSWParams(m=16, efconstruction=200),
        auto_build=False
    ),
    SecondaryIndex(index_name="author_idx", field="author"),
    FilteringIndex(index_name="filter_idx", fields=["bookName", "author"]),
]
table.create_indexes(indexes)

# 配置自动重建
time.sleep(1)
table.modify_index(
    index_name="vector_idx",
    auto_build=True,
    auto_build_index_policy=AutoBuildTiming("2024-12-01 03:00:00")
)

# 查看索引信息
index = table.describe_index("vector_idx")
print(f"索引: {index.to_dict()}")

# 手动重建
table.rebuild_index("vector_idx")
while table.describe_index("vector_idx").state != IndexState.NORMAL:
    time.sleep(2)
print("索引重建完成")
```

---

**相关文档**:
- [索引类型](../core-concepts/index-types.md)
- [向量检索](./vector-search.md)
- [混合检索](../advanced/hybrid-search.md)
