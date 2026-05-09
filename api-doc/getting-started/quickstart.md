# 快速开始

本教程将帮助你在 5 分钟内完成向量数据库的基本操作。

## 前置条件

1. 已安装 `pymochow` SDK（参见 [安装指南](./installation.md)）
2. 已获取 VectorDB 实例的 `endpoint` 和 `api_key`

## 1. 初始化客户端

```python
import pymochow
from pymochow.configuration import Configuration
from pymochow.auth.bce_credentials import BceCredentials

# 配置认证信息
account = 'root'
api_key = 'your_api_key'
endpoint = 'http://127.0.0.1:8511'

# 创建客户端
config = Configuration(
    credentials=BceCredentials(account, api_key),
    endpoint=endpoint
)
client = pymochow.MochowClient(config)
```

## 2. 创建数据库

```python
# 创建数据库
db = client.create_database('book')

# 查看数据库列表
for db_item in client.list_databases():
    print(f"数据库: {db_item.database_name}")
```

## 3. 创建表

```python
from pymochow.model.schema import Schema, Field, VectorIndex, HNSWParams
from pymochow.model.enum import FieldType, IndexType, MetricType
from pymochow.model.table import Partition

# 定义字段
fields = [
    Field("id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("title", FieldType.STRING, not_null=True),
    Field("content", FieldType.TEXT),
    Field("vector", FieldType.FLOAT_VECTOR, not_null=True, dimension=768),
]

# 定义向量索引
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
    table_name='documents',
    replication=3,
    partition=Partition(partition_num=1),
    schema=Schema(fields=fields, indexes=indexes)
)
```

## 4. 等待表就绪

```python
import time
from pymochow.model.enum import TableState

while True:
    time.sleep(2)
    table = db.describe_table('documents')
    if table.state == TableState.NORMAL:
        print("表已就绪")
        break
```

## 5. 插入数据

```python
from pymochow.model.table import Row

# 获取表对象
table = db.table('documents')

# 准备数据
rows = [
    Row(
        id='doc_001',
        title='VectorDB 简介',
        content='VectorDB 是一款企业级向量数据库...',
        vector=[0.1] * 768  # 768维向量
    ),
    Row(
        id='doc_002',
        title='向量检索原理',
        content='向量检索基于相似度计算...',
        vector=[0.2] * 768
    ),
]

# 插入数据
table.upsert(rows=rows)
```

## 6. 向量检索

```python
from pymochow.model.table import VectorTopkSearchRequest, FloatVector, VectorSearchConfig
from pymochow.model.enum import IndexState

# 重建索引
table.rebuild_index("vector_idx")

# 等待索引就绪
while True:
    time.sleep(2)
    index = table.describe_index("vector_idx")
    if index.state == IndexState.NORMAL:
        break

# 创建查询请求
query_vector = FloatVector([0.15] * 768)
request = VectorTopkSearchRequest(
    vector_field="vector",
    vector=query_vector,
    limit=10,
    config=VectorSearchConfig(ef=200)
)

# 执行检索
results = table.vector_search(request=request)
for row in results.rows:
    print(f"ID: {row.id}, Distance: {row.distance}")
```

## 7. 清理资源

```python
# 删除表
db.drop_table('documents')
time.sleep(10)

# 删除数据库
db.drop_database()

# 关闭客户端
client.close()
```

## 完整代码

```python
import time
import pymochow
from pymochow.configuration import Configuration
from pymochow.auth.bce_credentials import BceCredentials
from pymochow.model.schema import Schema, Field, VectorIndex, HNSWParams
from pymochow.model.enum import FieldType, IndexType, MetricType, TableState, IndexState
from pymochow.model.table import Partition, Row, VectorTopkSearchRequest, FloatVector, VectorSearchConfig

# 1. 初始化客户端
config = Configuration(
    credentials=BceCredentials('root', 'your_api_key'),
    endpoint='http://127.0.0.1:8511'
)
client = pymochow.MochowClient(config)

# 2. 创建数据库和表
db = client.create_database('book')
fields = [
    Field("id", FieldType.STRING, primary_key=True, partition_key=True, not_null=True),
    Field("title", FieldType.STRING, not_null=True),
    Field("vector", FieldType.FLOAT_VECTOR, not_null=True, dimension=768),
]
indexes = [
    VectorIndex("vector_idx", IndexType.HNSW, "vector", MetricType.L2, HNSWParams(m=32, efconstruction=200))
]
db.create_table('documents', replication=3, partition=Partition(1), schema=Schema(fields, indexes))

# 3. 等待表就绪并插入数据
while db.describe_table('documents').state != TableState.NORMAL:
    time.sleep(2)
table = db.table('documents')
table.upsert(rows=[Row(id='001', title='Test', vector=[0.1]*768)])

# 4. 重建索引并检索
table.rebuild_index("vector_idx")
while table.describe_index("vector_idx").state != IndexState.NORMAL:
    time.sleep(2)
results = table.vector_search(VectorTopkSearchRequest("vector", FloatVector([0.1]*768), 10))
print(results)

# 5. 清理
db.drop_table('documents')
time.sleep(10)
db.drop_database()
client.close()
```

---

**下一步**: 
- [数据模型](../core-concepts/data-model.md) - 了解字段类型和索引
- [向量检索](../api-reference/vector-search.md) - 深入了解检索 API
