# 典型场景

## 概述

本文档介绍 VectorDB 在常见业务场景中的应用示例。

---

## RAG 知识库

### 场景描述

构建一个基于向量检索的知识库问答系统（RAG: Retrieval-Augmented Generation）。

### 表结构设计

```python
from pymochow.model.schema import Schema, Field, VectorIndex, HNSWParams, InvertedIndex, InvertedIndexParams
from pymochow.model.enum import FieldType, IndexType, MetricType, InvertedIndexAnalyzer, InvertedIndexParseMode, InvertedIndexFieldAttribute

fields = [
    Field("doc_id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("chunk_id", FieldType.STRING, primary_key=True, not_null=True),
    Field("title", FieldType.STRING, not_null=True),
    Field("content", FieldType.TEXT, not_null=True),
    Field("source", FieldType.STRING),
    Field("embedding", FieldType.FLOAT_VECTOR, dimension=768, not_null=True),
]

indexes = [
    VectorIndex("embedding_idx", IndexType.HNSW, "embedding", MetricType.COSINE,
                HNSWParams(m=32, efconstruction=200)),
    InvertedIndex(
        index_name="content_idx",
        fields=["content"],
        params=InvertedIndexParams(
            analyzer=InvertedIndexAnalyzer.CHINESE_ANALYZER,
            parse_mode=InvertedIndexParseMode.COARSE_MODE
        ),
        field_attributes=[InvertedIndexFieldAttribute.ANALYZED]
    ),
]
```

### 检索示例

```python
def rag_search(table, question, embedding_model):
    """RAG 检索"""
    # 生成问题向量
    question_embedding = embedding_model.encode(question)
    
    # 混合检索
    from pymochow.model.table import (
        VectorTopkSearchRequest, BM25SearchRequest, HybridSearchRequest,
        FloatVector, VectorSearchConfig
    )
    
    vector_request = VectorTopkSearchRequest(
        vector_field="embedding",
        vector=FloatVector(question_embedding),
        limit=10,
        config=VectorSearchConfig(ef=200)
    )
    
    bm25_request = BM25SearchRequest(
        index_name="content_idx",
        search_text=question
    )
    
    hybrid_request = HybridSearchRequest(
        vector_request=vector_request,
        vector_weight=0.6,
        bm25_request=bm25_request,
        bm25_weight=0.4,
        limit=5
    )
    
    results = table.hybrid_search(request=hybrid_request)
    
    # 返回上下文
    contexts = [row.content for row in results.rows]
    return contexts
```

---

## 商品推荐

### 场景描述

基于商品特征向量的相似商品推荐。

### 表结构设计

```python
from pymochow.model.schema import FilteringIndex

fields = [
    Field("product_id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("name", FieldType.STRING, not_null=True),
    Field("category", FieldType.STRING, not_null=True),
    Field("price", FieldType.DOUBLE),
    Field("description", FieldType.TEXT),
    Field("image_embedding", FieldType.FLOAT_VECTOR, dimension=512, not_null=True),
    Field("text_embedding", FieldType.FLOAT_VECTOR, dimension=768),
]

indexes = [
    VectorIndex("image_idx", IndexType.HNSW, "image_embedding", MetricType.COSINE,
                HNSWParams(m=16, efconstruction=200)),
    VectorIndex("text_idx", IndexType.HNSW, "text_embedding", MetricType.COSINE,
                HNSWParams(m=16, efconstruction=200)),
    FilteringIndex("filter_idx", fields=["category", "price"]),
]
```

### 推荐示例

```python
def recommend_similar_products(table, product_id, category=None, top_k=10):
    """推荐相似商品"""
    # 查询原商品
    product = table.query(
        primary_key={'product_id': product_id},
        projections=['image_embedding'],
        retrieve_vector=True
    )
    
    # 构建检索请求
    filter_condition = f"product_id!='{product_id}'"
    if category:
        filter_condition += f" AND category='{category}'"
    
    request = VectorTopkSearchRequest(
        vector_field="image_embedding",
        vector=FloatVector(product.image_embedding),
        limit=top_k,
        filter=filter_condition,
        config=VectorSearchConfig(ef=200)
    )
    
    results = table.vector_search(
        request=request,
        projections=['product_id', 'name', 'category', 'price']
    )
    
    return results.rows
```

---

## 多租户系统

### 场景描述

为每个租户隔离数据，同时共享基础设施。

### 表结构设计

```python
from pymochow.model.schema import FilteringIndex

fields = [
    # 使用 tenant_id 作为分区键
    Field("tenant_id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("doc_id", FieldType.STRING, primary_key=True, not_null=True),
    Field("title", FieldType.STRING, not_null=True),
    Field("content", FieldType.TEXT),
    Field("embedding", FieldType.FLOAT_VECTOR, dimension=768, not_null=True),
]

indexes = [
    VectorIndex("embedding_idx", IndexType.HNSW, "embedding", MetricType.L2,
                HNSWParams(m=32, efconstruction=200)),
    FilteringIndex("filter_idx", fields=["tenant_id", "title"]),
]
```

### 租户隔离检索

```python
def search_tenant_documents(table, tenant_id, query_vector, top_k=10):
    """在指定租户范围内检索"""
    request = VectorTopkSearchRequest(
        vector_field="embedding",
        vector=FloatVector(query_vector),
        limit=top_k,
        filter=f"tenant_id='{tenant_id}'",  # 租户隔离
        config=VectorSearchConfig(ef=200)
    )
    
    results = table.vector_search(
        request=request,
        projections=['doc_id', 'title']
    )
    
    return results.rows
```

---

## 图像搜索

### 场景描述

基于图像特征向量的以图搜图。

### 表结构设计

```python
fields = [
    Field("image_id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("url", FieldType.STRING, not_null=True),
    Field("tags", FieldType.ARRAY, element_type=ElementType.STRING),
    Field("metadata", FieldType.JSON),
    Field("feature", FieldType.FLOAT_VECTOR, dimension=2048, not_null=True),
]

indexes = [
    VectorIndex("feature_idx", IndexType.HNSW, "feature", MetricType.COSINE,
                HNSWParams(m=32, efconstruction=200)),
]
```

### 以图搜图

```python
def search_similar_images(table, image_feature, top_k=20):
    """以图搜图"""
    request = VectorTopkSearchRequest(
        vector_field="feature",
        vector=FloatVector(image_feature),
        limit=top_k,
        config=VectorSearchConfig(ef=200)
    )
    
    results = table.vector_search(
        request=request,
        projections=['image_id', 'url', 'tags']
    )
    
    return [{'id': row.image_id, 'url': row.url, 'score': row.score} 
            for row in results.rows]
```

---

## 日志分析

### 场景描述

对日志文本进行语义分析和异常检测。

### 表结构设计

```python
from pymochow.model.schema import HNSWPQParams, FilteringIndex

fields = [
    Field("log_id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("timestamp", FieldType.DATETIME, not_null=True),
    Field("level", FieldType.STRING, not_null=True),  # INFO, WARN, ERROR
    Field("service", FieldType.STRING, not_null=True),
    Field("message", FieldType.TEXT, not_null=True),
    Field("embedding", FieldType.FLOAT_VECTOR, dimension=384, not_null=True),
]

indexes = [
    VectorIndex("embedding_idx", IndexType.HNSWPQ, "embedding", MetricType.L2,
                HNSWPQParams(m=16, efconstruction=200, NSQ=48, samplerate=0.5)),
    FilteringIndex("filter_idx", fields=["level", "service", "timestamp"]),
]
```

### 异常检测

```python
def find_similar_errors(table, error_embedding, service=None, top_k=50):
    """查找相似的错误日志"""
    filter_condition = "level='ERROR'"
    if service:
        filter_condition += f" AND service='{service}'"
    
    request = VectorTopkSearchRequest(
        vector_field="embedding",
        vector=FloatVector(error_embedding),
        limit=top_k,
        filter=filter_condition,
        config=VectorSearchConfig(ef=200)
    )
    
    results = table.vector_search(
        request=request,
        projections=['log_id', 'timestamp', 'service', 'message']
    )
    
    return results.rows
```

---

## 场景选型建议

| 场景 | 推荐索引 | 关键配置 |
|------|----------|----------|
| RAG 知识库 | HNSW + 倒排索引 | 混合检索 |
| 商品推荐 | HNSW | 多向量检索 |
| 多租户 | HNSW + 过滤索引 | 分区键 = tenant_id |
| 图像搜索 | HNSW | COSINE 度量 |
| 日志分析 | HNSWPQ | 大规模降本 |

---

**相关文档**:
- [向量检索](../api-reference/vector-search.md)
- [混合检索](../advanced/hybrid-search.md)
- [最佳实践](../best-practices/overview.md)
