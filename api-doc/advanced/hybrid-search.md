# 混合检索

## 概述

混合检索结合向量检索和 BM25 文本检索，实现更精确的语义匹配。

---

## BM25 检索

### BM25SearchRequest

基于倒排索引的全文检索。

```python
from pymochow.model.table import BM25SearchRequest

request = BM25SearchRequest(
    index_name="book_segment_inverted_idx",
    search_text="吕布",
    limit=10,
    filter="bookName='三国演义'"
)

result = table.bm25_search(request=request)
for row in result.rows:
    print(f"ID: {row.id}, Score: {row.score}")
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `index_name` | `str` | 是 | 倒排索引名称 |
| `search_text` | `str` | 是 | 检索文本 |
| `limit` | `int` | 否 | 返回数量 |
| `filter` | `str` | 否 | 过滤条件 |

---

## 混合检索

### HybridSearchRequest

结合向量检索和 BM25 检索，通过权重融合结果。

```python
from pymochow.model.table import (
    VectorTopkSearchRequest,
    BM25SearchRequest,
    HybridSearchRequest,
    FloatVector,
    VectorSearchConfig
)

# 向量检索请求
vector_request = VectorTopkSearchRequest(
    vector_field="vector",
    vector=FloatVector([1, 0.21, 0.213, 0]),
    limit=10,
    config=VectorSearchConfig(ef=200, pruning=True)
)

# BM25 检索请求
bm25_request = BM25SearchRequest(
    index_name="book_segment_inverted_idx",
    search_text="吕布"
)

# 混合检索
hybrid_request = HybridSearchRequest(
    vector_request=vector_request,
    vector_weight=0.4,
    bm25_request=bm25_request,
    bm25_weight=0.6,
    filter="bookName='三国演义'",
    limit=15
)

result = table.hybrid_search(request=hybrid_request)
for row in result.rows:
    print(f"ID: {row.id}, Score: {row.score}")
```

**参数**:

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `vector_request` | `VectorTopkSearchRequest` | 是 | 向量检索请求 |
| `vector_weight` | `float` | 是 | 向量检索权重 |
| `bm25_request` | `BM25SearchRequest` | 是 | BM25 检索请求 |
| `bm25_weight` | `float` | 是 | BM25 检索权重 |
| `filter` | `str` | 否 | 过滤条件 |
| `limit` | `int` | 否 | 返回数量 |

---

## 倒排索引配置

使用 BM25 检索前需要创建倒排索引：

```python
from pymochow.model.schema import InvertedIndex, InvertedIndexParams
from pymochow.model.enum import (
    InvertedIndexAnalyzer,
    InvertedIndexParseMode,
    InvertedIndexFieldAttribute
)

index = InvertedIndex(
    index_name="content_inverted_idx",
    fields=["segment"],
    params=InvertedIndexParams(
        analyzer=InvertedIndexAnalyzer.CHINESE_ANALYZER,
        parse_mode=InvertedIndexParseMode.COARSE_MODE,
        case_sensitive=True
    ),
    field_attributes=[InvertedIndexFieldAttribute.ANALYZED]
)
```

### 分词器选项

| Analyzer | 说明 |
|----------|------|
| `CHINESE_ANALYZER` | 中文分词器 |
| `STANDARD_ANALYZER` | 标准分词器 |

### 解析模式

| ParseMode | 说明 |
|-----------|------|
| `COARSE_MODE` | 粗粒度分词 |
| `FINE_MODE` | 细粒度分词 |

---

## 权重设置建议

| 场景 | 向量权重 | BM25权重 | 说明 |
|------|----------|----------|------|
| 语义优先 | 0.7 | 0.3 | 当语义相似性更重要时 |
| 关键词优先 | 0.3 | 0.7 | 当精确匹配更重要时 |
| 平衡 | 0.5 | 0.5 | 两者同等重要 |

---

## 完整示例

```python
import time
from pymochow.model.schema import (
    Schema, Field, VectorIndex, HNSWParams, InvertedIndex, InvertedIndexParams
)
from pymochow.model.enum import (
    FieldType, IndexType, MetricType, TableState, IndexState,
    InvertedIndexAnalyzer, InvertedIndexParseMode, InvertedIndexFieldAttribute
)
from pymochow.model.table import (
    Partition, Row,
    VectorTopkSearchRequest, BM25SearchRequest, HybridSearchRequest,
    FloatVector, VectorSearchConfig
)

# 1. 创建带倒排索引的表
fields = [
    Field("id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("title", FieldType.STRING, not_null=True),
    Field("content", FieldType.TEXT),
    Field("vector", FieldType.FLOAT_VECTOR, not_null=True, dimension=4),
]

indexes = [
    VectorIndex("vector_idx", IndexType.HNSW, "vector", MetricType.L2, 
                HNSWParams(m=32, efconstruction=200)),
    InvertedIndex(
        index_name="content_idx",
        fields=["content"],
        params=InvertedIndexParams(
            analyzer=InvertedIndexAnalyzer.CHINESE_ANALYZER,
            parse_mode=InvertedIndexParseMode.COARSE_MODE,
            case_sensitive=True
        ),
        field_attributes=[InvertedIndexFieldAttribute.ANALYZED]
    ),
]

db.create_table("hybrid_demo", replication=3, partition=Partition(1), 
                schema=Schema(fields, indexes))

while db.describe_table("hybrid_demo").state != TableState.NORMAL:
    time.sleep(2)

# 2. 插入数据
table = db.table("hybrid_demo")
rows = [
    Row(id="1", title="三国演义", content="布大惊，与陈宫商议", vector=[1, 0.1, 0.2, 0]),
    Row(id="2", title="三国演义", content="吕布乃虎狼之徒", vector=[2, 0.2, 0.3, 0]),
]
table.upsert(rows=rows)
time.sleep(2)

# 3. 重建向量索引
table.rebuild_index("vector_idx")
while table.describe_index("vector_idx").state != IndexState.NORMAL:
    time.sleep(2)

# 4. BM25 检索
bm25_result = table.bm25_search(
    BM25SearchRequest(index_name="content_idx", search_text="吕布", limit=10)
)
print("BM25 结果:", bm25_result)

# 5. 混合检索
vector_request = VectorTopkSearchRequest(
    vector_field="vector", 
    vector=FloatVector([1, 0.1, 0.2, 0]), 
    limit=10,
    config=VectorSearchConfig(ef=200)
)
bm25_request = BM25SearchRequest(index_name="content_idx", search_text="吕布")

hybrid_result = table.hybrid_search(
    HybridSearchRequest(
        vector_request=vector_request,
        vector_weight=0.4,
        bm25_request=bm25_request,
        bm25_weight=0.6,
        limit=10
    )
)
print("混合检索结果:", hybrid_result)
```

---

## 使用场景

### RAG 知识库

结合语义相似性和关键词匹配，提高问答准确性：

```python
# 用户问题
question = "吕布是怎么死的"
question_embedding = get_embedding(question)  # 生成向量

# 混合检索
hybrid_request = HybridSearchRequest(
    vector_request=VectorTopkSearchRequest(
        vector_field="embedding",
        vector=FloatVector(question_embedding),
        limit=10,
        config=VectorSearchConfig(ef=200)
    ),
    vector_weight=0.5,
    bm25_request=BM25SearchRequest(
        index_name="content_idx",
        search_text=question
    ),
    bm25_weight=0.5,
    limit=5
)

results = table.hybrid_search(request=hybrid_request)
```

---

**相关文档**:
- [向量检索](../api-reference/vector-search.md)
- [索引操作](../api-reference/index-operations.md)
- [索引类型](../core-concepts/index-types.md)
