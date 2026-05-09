# 最佳实践指南

## 目录

- [Schema 设计](#schema-设计)
- [索引设计](#索引设计)
- [数据操作](#数据操作)
- [性能优化](#性能优化)
- [成本优化](#成本优化)
- [安全建议](#安全建议)

---

## Schema 设计

### 主键设计

**推荐做法**：
- 使用 UUID 或业务唯一标识作为主键
- 主键总长度控制在 100 字节以内
- **禁止使用自增主键**

```python
import uuid

def generate_id():
    return str(uuid.uuid4())
```

### 分区键设计

| 业务场景 | 推荐分区键 | 说明 |
|----------|------------|------|
| 电商商品搜索 | `category_id` | 按类目检索 |
| 企业内部文档 | `department_id` | 按部门隔离 |
| 多租户应用 | `tenant_id` | 按租户隔离 |
| 日志分析 | `date` | 按日期分区 |
| 全局搜索 | 无（单分片） | 小规模数据 |

### 分区数选择

| 数据规模 | 建议分区数 |
|----------|------------|
| < 10万 | 1 |
| 10万-100万 | 4 |
| 100万-1000万 | 16 |
| 1000万-5000万 | 64 |
| > 5000万 | 128+ |

### 字段设计

```python
schema = Schema(
    fields=[
        Field("id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
        Field("title", FieldType.STRING, not_null=True),
        Field("content", FieldType.TEXT),          # 大文本
        Field("metadata", FieldType.JSON),          # 灵活字段使用 JSON
        Field("category", FieldType.STRING),        # 频繁过滤字段
        Field("embedding", FieldType.FLOAT_VECTOR, dimension=768, not_null=True),
    ],
    indexes=[...]
)
```

---

## 索引设计

### 向量索引选择

| 数据规模 | 推荐索引 | 说明 |
|----------|----------|------|
| < 100万 | HNSW | 高精度 |
| 100万-1000万 | HNSW/HNSWPQ | 平衡精度和成本 |
| 1000万-1亿 | HNSWPQ/HNSWSQ | 降本 |
| > 1亿 | DISKANN | 超大规模 |

### 过滤索引设计

> **强制**: 过滤条件和投影中使用的字段必须建立过滤索引！

```python
FilteringIndex(
    index_name="filter_idx",
    fields=["category", "status", "author_id"]
)
```

### 索引自动重建策略

| 场景 | 推荐策略 |
|------|----------|
| 24小时持续写入 | `AutoBuildRowCountIncrement` |
| 有低峰期 | `AutoBuildPeriodical` |
| 批量导入 | `AutoBuildPeriodical` + 手动重建 |

---

## 数据操作

### 批量插入

```python
BATCH_SIZE = 1000

def batch_insert(data, table):
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        table.upsert(rows=batch)
```

### 大数据量导入

```python
def migrate_data(source_table, target_table):
    # 1. 创建新表
    # 2. 分批导入数据
    for batch in iterate_batches(source_table, batch_size=1000):
        target_table.upsert(rows=batch)
    # 3. 重建索引
    target_table.rebuild_index("vector_idx")
    # 4. 验证数据
    # 5. 切换流量
```

### 安全删除

> **禁止** 大范围 delete 操作，推荐分批删除。

```python
def safe_delete(table, filter_condition, batch_size=1000):
    while True:
        result = table.select(filter=filter_condition, projections=["id"], limit=batch_size)
        if not result.rows:
            break
        for row in result.rows:
            table.delete(primary_key={"id": row.id})
```

---

## 性能优化

### 查询优化

**1. 缩小检索范围**

```python
# 坏：全量检索
table.vector_search(request)

# 好：带过滤的检索
request = VectorTopkSearchRequest(..., filter="category='tech' AND status='published'")
```

**2. 只返回必要字段**

```python
# 坏：返回所有字段
table.vector_search(request, projections=["*"])

# 好：只返回必要字段
table.vector_search(request, projections=["id", "title"])
```

**3. 不要返回向量**

```python
# 向量字段很大，会影响性能
table.query(primary_key={"id": "001"}, retrieve_vector=False)
```

### 索引参数调优

**提高召回率**:

```python
HNSWParams(m=32, efconstruction=400)
VectorSearchConfig(ef=256)
```

**降低内存占用**:

```python
HNSWParams(m=8, efconstruction=100)
VectorSearchConfig(ef=64)
```

---

## 成本优化

### 索引类型选择

| 数据规模 | 推荐索引 | 内存估算 |
|----------|----------|----------|
| < 100万 | HNSW | ~向量大小 × 4 |
| 100万-1000万 | HNSWPQ | ~向量大小 × 2 |
| 1000万-1亿 | HNSWSQ/DISKANN | ~向量大小 × 1 |
| > 1亿 | DISKANN | ~向量大小 × 0.5 |

### 批量操作

```python
# 批量操作比单条操作更高效
# 单条插入：1000条需要 1000 次网络往返
# 批量插入：1000条只需 1 次网络往返
```

---

## 安全建议

### AK/SK 管理

```python
import os

# 坏：硬编码
credentials = BceCredentials("root", "hard_coded_key")

# 好：使用环境变量
credentials = BceCredentials(
    account=os.getenv("MOCHOW_ACCOUNT", "root"),
    api_key=os.getenv("MOCHOW_API_KEY")
)
```

### 最小权限原则

- 创建只读账号用于查询
- 创建读写账号用于数据导入
- 定期轮换 AK/SK（建议每 90 天）

### 审计日志

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pymochow")

# 记录敏感操作
logger.info(f"Create table: {table_name}")
logger.info(f"Insert rows: {count}")
```

---

## 监控建议

### 关键指标

| 指标 | 告警阈值 | 说明 |
|------|----------|------|
| P99 延迟 | > 500ms | 慢查询检测 |
| 错误率 | > 1% | 服务异常检测 |
| CU 使用率 | > 80% | 资源不足预警 |
| 索引构建时间 | > 1小时 | 数据积压预警 |

### 监控命令

```python
# 查看表统计信息
stats = table.stats()
print(f"统计信息: {stats}")

# 查看索引状态
index = table.describe_index("vector_idx")
print(f"索引状态: {index.state}")
```

---

**相关文档**:
- [数据模型](../core-concepts/data-model.md)
- [索引类型](../core-concepts/index-types.md)
- [错误码参考](../troubleshooting/error-codes.md)
