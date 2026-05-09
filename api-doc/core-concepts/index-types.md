# 索引类型

## 概述

索引是加速相似性检索的关键组件。VectorDB 提供多种向量索引和标量索引类型。

---

## 向量索引对比

| 索引类型 | 精度 | 性能 | 内存占用 | 适用场景 |
|----------|------|------|----------|----------|
| **HNSW** | 高 | 高 | 高 | 通用场景，推荐首选 |
| **HNSWPQ** | 中 | 高 | 中 | 大规模数据，降本场景 |
| **HNSWSQ** | 中 | 高 | 中 | 内存受限场景 |
| **DISKANN** | 中 | 中 | 低 | 超大规模数据（亿级） |
| **IVF** | 中 | 中 | 低 | 超大规模数据 |
| **IVFSQ** | 中 | 中 | 低 | 超大规模数据+量化 |
| **PUCK** | 中 | 中 | 中 | 特定场景 |
| **FLAT** | 最高 | 低 | 高 | 小规模精确检索 |

---

## HNSW

**Hierarchical Navigable Small World** - 业界最常用的向量索引。

### 特点

- 高精度、高查询速度
- 内存占用较高
- 适合大多数场景

### 构建参数

| 参数 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| `m` | 16 | 4-64 | 每个节点的邻居数，越大精度越高 |
| `efconstruction` | 200 | 100-400 | 构建时搜索广度，越大构建越慢但更精确 |

### 检索参数

| 参数 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| `ef` | 100 | 10-1000 | 检索时搜索广度，越大精度越高 |

### 示例

```python
from pymochow.model.schema import VectorIndex, HNSWParams
from pymochow.model.enum import IndexType, MetricType
from pymochow.model.table import VectorSearchConfig

# 创建索引
index = VectorIndex(
    index_name="vector_idx",
    index_type=IndexType.HNSW,
    field="vector",
    metric_type=MetricType.L2,
    params=HNSWParams(m=32, efconstruction=200)
)

# 检索配置
config = VectorSearchConfig(ef=200, pruning=True)
```

---

## HNSWPQ

**HNSW with Product Quantization** - 在 HNSW 基础上使用 PQ 压缩降低内存。

### 特点

- 内存占用约为 HNSW 的 1/2
- 牺牲一定召回率
- 适合大规模数据降本场景

### 参数

| 参数 | 说明 |
|------|------|
| `m` | HNSW 参数 |
| `efconstruction` | HNSW 参数 |
| `NSQ` | 子向量数量，通常为维度/4 或维度/8 |
| `samplerate` | 采样率，通常 0.5-1.0 |

### 示例

```python
from pymochow.model.schema import VectorIndex, HNSWPQParams

index = VectorIndex(
    index_name="vector_idx",
    index_type=IndexType.HNSWPQ,
    field="vector",
    metric_type=MetricType.L2,
    params=HNSWPQParams(m=16, efconstruction=200, NSQ=4, samplerate=1.0)
)
```

---

## HNSWSQ

**HNSW with Scalar Quantization** - 使用标量量化压缩。

### 特点

- 将 float32 转为 int8
- 内存占用约为 HNSW 的 1/4
- 精度损失较小

### 参数

| 参数 | 说明 |
|------|------|
| `m` | HNSW 参数 |
| `efconstruction` | HNSW 参数 |
| `qtBits` | 量化位数，通常为 8 |

### 示例

```python
from pymochow.model.schema import VectorIndex, HNSWSQParams

index = VectorIndex(
    index_name="vector_idx",
    index_type=IndexType.HNSWSQ,
    field="vector",
    metric_type=MetricType.IP,
    params=HNSWSQParams(m=16, efconstruction=200, qtBits=8)
)
```

---

## PUCK

### 参数

| 参数 | 说明 |
|------|------|
| `coarseClusterCount` | 粗聚类数量 |
| `fineClusterCount` | 细聚类数量 |

### 示例

```python
from pymochow.model.schema import VectorIndex, PUCKParams

index = VectorIndex(
    index_name="vector_idx",
    index_type=IndexType.PUCK,
    field="vector",
    metric_type=MetricType.L2,
    params=PUCKParams(coarseClusterCount=5, fineClusterCount=5)
)

# 检索配置
config = VectorSearchConfig(search_coarse_count=5)
```

---

## DISKANN

基于磁盘的向量索引，适合超大规模数据。

### 特点

- 内存占用极低
- 适合亿级数据
- 查询性能稍低于 HNSW

### 构建参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `NSQ` | 512 | 子空间数量 |
| `R` | 64 | 候选列表大小 |
| `L` | 100 | 图层大小 |

### 检索参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `w` | 1 | beam search 宽度 |
| `search_l` | 100 | 搜索范围 |

### 示例

```python
from pymochow.model.schema import VectorIndex, DISKANNParams

index = VectorIndex(
    index_name="vector_idx",
    index_type=IndexType.DISKANN,
    field="vector",
    metric_type=MetricType.L2,
    params=DISKANNParams(NSQ=4, R=64, L=100)
)

# 检索配置
config = VectorSearchConfig(w=1, search_l=100)
```

---

## 稀疏向量索引

### SPARSE_OPTIMIZED_FLAT

用于稀疏向量的索引类型。

```python
index = VectorIndex(
    index_name="sparse_vector_idx",
    index_type=IndexType.SPARSE_OPTIMIZED_FLAT,
    field="sparse_vector",
    metric_type=MetricType.IP
)
```

---

## 索引选择决策树

```
数据量 < 1000万?
├─ 是 → 需要高精度? → 是 → HNSW
│                   └─ 否 → HNSWPQ
└─ 否 → 内存充足? → 是 → HNSW
                  └─ 否 → 数据量 < 1亿?
                            ├─ 是 → HNSWPQ/HNSWSQ
                            └─ 否 → DISKANN
```

---

## 标量索引

### 二级索引 (SecondaryIndex)

用于加速标量字段的查询。

```python
from pymochow.model.schema import SecondaryIndex

index = SecondaryIndex(
    index_name="book_name_idx",
    field="bookName"
)
```

### 过滤索引 (FilteringIndex)

用于加速过滤条件和投影字段。

```python
from pymochow.model.schema import FilteringIndex

index = FilteringIndex(
    index_name="filter_idx",
    fields=["bookName", "author", "status"]
)
```

> **重要**: 过滤条件和投影中使用的字段必须建立过滤索引！

### 倒排索引 (InvertedIndex)

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

---

## 索引状态

| 状态 | 说明 |
|------|------|
| `INVALID` | 索引无效或刚创建 |
| `BUILDING` | 正在构建中 |
| `NORMAL` | 索引可用 |

---

## 自动重建策略

| 策略 | 说明 | 适用场景 |
|------|------|----------|
| `AutoBuildTiming` | 指定时间触发 | 每天定时导入 |
| `AutoBuildPeriodical` | 周期性触发 | 有低峰期的场景 |
| `AutoBuildRowCountIncrement` | 增量行数触发 | 24小时持续写入 |

```python
from pymochow.model.schema import AutoBuildTiming

table.modify_index(
    index_name="vector_idx",
    auto_build=True,
    auto_build_index_policy=AutoBuildTiming("2024-01-01 03:00:00")
)
```

---

## 索引参数调优

### 提高召回率

```python
HNSWParams(
    m=32,               # 增大 M
    efconstruction=400  # 增大 efConstruction
)

# 检索时
VectorSearchConfig(ef=256)  # 增大 ef
```

### 降低内存占用

```python
HNSWParams(
    m=8,                # 减小 M
    efconstruction=100  # 减小 efConstruction
)

# 或切换到 HNSWPQ/HNSWSQ/DISKANN
```

---

## 常见问题

### Q: 索引构建很慢？

检查数据量、分区数、HNSW 参数设置。可考虑减小 M 和 efConstruction。

### Q: 召回率不理想？

尝试增大 HNSW 的 M、efConstruction、ef 参数，或从 HNSWPQ 切换到 HNSW。

### Q: 内存占用过高？

考虑使用 HNSWPQ、HNSWSQ 或 DISKANN 索引。

---

**相关文档**:
- [数据模型](./data-model.md)
- [索引操作](../api-reference/index-operations.md)
- [向量检索](../api-reference/vector-search.md)
