# 数据模型

## 概述

VectorDB 的数据模型包括字段类型、主键、分区键和向量字段。理解这些概念是设计高效表结构的基础。

---

## 字段类型

| 类型 | FieldType | 说明 | 示例 |
|------|-----------|------|------|
| 布尔值 | `BOOL` | 真/假 | `True` |
| 8位整数 | `INT8` | -128 ~ 127 | `10` |
| 8位无符号整数 | `UINT8` | 0 ~ 255 | `200` |
| 16位整数 | `INT16` | -32768 ~ 32767 | `1000` |
| 16位无符号整数 | `UINT16` | 0 ~ 65535 | `50000` |
| 32位整数 | `INT32` | -2^31 ~ 2^31-1 | `-100000` |
| 32位无符号整数 | `UINT32` | 0 ~ 4,294,967,295 | `100` |
| 64位整数 | `INT64` | -2^63 ~ 2^63-1 | `-42` |
| 64位无符号整数 | `UINT64` | 0 ~ 2^64-1 | `1000000` |
| 单精度浮点 | `FLOAT` | 32位浮点数 | `3.14` |
| 双精度浮点 | `DOUBLE` | 64位浮点数 | `3.14159` |
| 日期 | `DATE` | 日期 | `"2024-01-01"` |
| 日期时间 | `DATETIME` | 日期时间 | `"2024-01-01T00:00:00Z"` |
| 时间戳 | `TIMESTAMP` | Unix 时间戳 | `1704067200` |
| 字符串 | `STRING` | 不定长字符串 | `"hello"` |
| 二进制 | `BINARY` | 二进制数据 | 二进制数据 |
| UUID | `UUID` | 唯一标识符 | `"550e8400-..."` |
| 长文本 | `TEXT` | UTF-8 编码文本 | 文章内容 |
| GBK文本 | `TEXT_GBK` | GBK 编码文本 | 中文文本 |
| GB18030文本 | `TEXT_GB18030` | GB18030 编码文本 | 中文文本 |
| 数组 | `ARRAY` | 同类型元素数组 | `["a", "b"]` |
| JSON | `JSON` | JSON 对象 | `{"key": "value"}` |
| 映射 | `MAP` | 键值对映射 | `{"key": "value"}` |
| 稠密向量 | `FLOAT_VECTOR` | 定长浮点向量 | `[0.1, 0.2, ...]` |
| 二进制向量 | `BINARY_VECTOR` | 二进制向量 | 二进制数据 |
| 稀疏向量 | `SPARSE_FLOAT_VECTOR` | 稀疏表示向量 | `{1: 0.5, 100: 0.3}` |

---

## 主键 (Primary Key)

主键用于唯一标识表中的每条记录。

### 规则

| 规则 | 说明 |
|------|------|
| 数量限制 | 最多 10 个主键字段 |
| 长度限制 | 主键总长度不超过 1KB |
| 非空 | 主键字段必须设置 `not_null=True` |
| 不可更改 | 主键值插入后不可修改 |

### 示例

```python
from pymochow.model.schema import Field
from pymochow.model.enum import FieldType

# 单主键
Field("id", FieldType.STRING, primary_key=True, not_null=True)

# 多主键（复合主键）
Field("tenant_id", FieldType.STRING, primary_key=True, not_null=True)
Field("doc_id", FieldType.STRING, primary_key=True, not_null=True)
```

### 最佳实践

> **警告**: 禁止使用自增主键。建议使用 UUID 或业务唯一标识。

```python
import uuid

def generate_id():
    return str(uuid.uuid4())
```

---

## 分区键 (Partition Key)

分区键用于将数据分布到不同的分片。

### 规则

| 规则 | 说明 |
|------|------|
| 单一字段 | 仅支持一个字段作为分区键 |
| 必须是主键 | 分区键必须从主键字段中选取 |
| 不可更改 | 分区键设置后无法修改 |
| 类型限制 | 不支持 BOOL/FLOAT/DOUBLE/FLOAT_VECTOR/BINARY_VECTOR/SPARSE_FLOAT_VECTOR/MAP/ARRAY/JSON |

### 选择原则

| 场景 | 推荐分区键 | 原因 |
|------|------------|------|
| 用户数据 | `user_id` | 按用户检索 |
| 多租户 | `tenant_id` | 按租户隔离 |
| 地域数据 | `region_id` | 按地域分布 |
| 全局搜索 | `id` | 无特定过滤需求 |

### 示例

```python
# 设置 id 为分区键
Field("id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True)
```

---

## 向量字段

### 稠密向量 (FLOAT_VECTOR)

最常用的向量类型，用于存储 embedding 向量。

```python
Field("vector", FieldType.FLOAT_VECTOR, dimension=768, not_null=True)
```

| 参数 | 说明 |
|------|------|
| `dimension` | 向量维度，范围 2-4096 |
| `not_null` | 建议设置为 True |

### 二进制向量 (BINARY_VECTOR)

用于存储二进制表示的向量。

```python
Field("binary_vec", FieldType.BINARY_VECTOR, dimension=128, not_null=True)
```

### 稀疏向量 (SPARSE_FLOAT_VECTOR)

用于存储稀疏表示的向量，如 TF-IDF、BM25 向量。

```python
Field("sparse_vec", FieldType.SPARSE_FLOAT_VECTOR, not_null=True)
```

---

## 度量类型

向量检索支持以下度量类型：

| 类型 | 说明 | 适用场景 |
|------|------|----------|
| `L2` | 欧氏距离 | 通用场景 |
| `IP` | 内积 | 归一化向量 |
| `COSINE` | 余弦相似度 | 文本、图像相似度 |

---

## 数组类型

数组类型需要指定元素类型：

```python
from pymochow.model.enum import ElementType

Field("tags", FieldType.ARRAY, element_type=ElementType.STRING, not_null=True)
```

### 元素类型

| ElementType | 说明 |
|-------------|------|
| `STRING` | 字符串数组 |
| `UINT32` | 整数数组 |
| `DOUBLE` | 浮点数组 |

---

## JSON 类型

用于存储半结构化数据：

```python
Field("metadata", FieldType.JSON)
```

### 使用示例

```python
from pymochow.model.table import Row

row = Row(
    id='001',
    metadata={
        'category': 'electronics',
        'brand': 'BrandA',
        'tags': ['sale', 'featured'],
        'price': 99.99
    }
)
```

---

## 数据分片

### 分区数建议

| 数据规模 | 建议分区数 |
|----------|------------|
| 1-10万行 | 1 |
| 10万-100万行 | 4 |
| 100万-1000万行 | 16 |
| 1000万-5000万行 | 64 |
| 5000万-1亿行 | 128 |

### 限制

- 单分片数据量不超过 1000 万行
- 分片数一旦设置不可更改

---

## 数据一致性

| 级别 | 说明 | 性能 |
|------|------|------|
| `STRONG` | 强一致，写入后立即可查 | 较低 |
| `EVENTUAL` | 最终一致，延迟更低 | 较高 |

> **建议**: 大多数场景使用 `EVENTUAL`，仅在有明确强一致需求时使用 `STRONG`。

---

## 完整表定义示例

```python
from pymochow.model.schema import Schema, Field, VectorIndex, HNSWParams, FilteringIndex
from pymochow.model.enum import FieldType, IndexType, MetricType, ElementType
from pymochow.model.table import Partition

# 定义字段
fields = [
    # 主键和分区键
    Field("id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    
    # 标量字段
    Field("title", FieldType.STRING, not_null=True),
    Field("author", FieldType.STRING),
    Field("page_count", FieldType.UINT32),
    Field("content", FieldType.TEXT),
    
    # 向量字段
    Field("embedding", FieldType.FLOAT_VECTOR, dimension=768, not_null=True),
    
    # 复杂类型
    Field("tags", FieldType.ARRAY, element_type=ElementType.STRING),
    Field("metadata", FieldType.JSON),
]

# 定义索引
indexes = [
    VectorIndex("embedding_idx", IndexType.HNSW, "embedding", MetricType.L2, 
                HNSWParams(m=32, efconstruction=200)),
    FilteringIndex("filter_idx", fields=["title", "author"]),
]

# 创建表
db.create_table(
    table_name='documents',
    replication=3,
    partition=Partition(partition_num=4),
    schema=Schema(fields=fields, indexes=indexes)
)
```

---

**相关文档**:
- [索引类型](./index-types.md)
- [表操作](../api-reference/table.md)
- [最佳实践](../best-practices/overview.md)
