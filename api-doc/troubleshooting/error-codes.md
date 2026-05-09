# 错误码参考

## 概述

本文档列出 VectorDB SDK 的常见错误码及解决方案。

---

## 数据库操作

### Create Database

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Invalid Parameter | 数据库名称为空或不是字符串 | 填写有效的数据库名称 |
| Privilege Denied | 无创建数据库权限 | 联系管理员授权 |
| Database Already Exists | 数据库已存在 | 使用不同的数据库名称 |
| Illegal Database Name | 数据库名称不合法 | 使用字母开头，符合命名规范 |

### List Database

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Privilege Denied | 无查询权限 | 联系管理员授权 |
| RPC Authentication Failed | 与 master 服务认证失败 | 检查 AK/SK 配置 |
| Internal Server Error | 后端服务内部错误 | 联系技术支持 |

### Drop Database

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Empty Database Name | 未指定数据库名称 | 必须指定数据库名称 |
| Database Not Exist | 数据库不存在 | 检查数据库名称 |
| Database Not Empty | 数据库非空 | 先删除所有表再删库 |

---

## 表操作

### Create Table

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Invalid Parameter | 表名称无效 | 检查表名称格式 |
| Invalid Field Type | 字段类型不支持 | 使用正确的数据类型 |
| Dimension Required | 向量字段缺少维度 | 为向量字段设置 `dimension` |
| Invalid Dimension | 向量维度超出范围 | 确保维度在 [2, 4096] |
| Primary Key Cannot Be Null | 主键字段可空 | 设置主键为 `not_null=True` |
| Invalid Auto Increment Type | 自增字段类型错误 | 自增字段必须为 UINT64 |
| Table Already Exists | 表已存在 | 使用不同的表名称 |
| Privilege Denied | 无创建表权限 | 联系管理员授权 |
| ARRAY Missing Element Type | ARRAY 类型缺少元素类型 | 设置 `element_type` |
| Map Field Missing Key/Value Type | MAP 类型缺少类型定义 | 设置 keyType 和 valueType |

### Describe Table

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Table Not Found | 表不存在 | 检查数据库和表名称 |
| Privilege Denied | 无查询权限 | 联系管理员授权 |

### Drop Table

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Table State Invalid | 表正在删除中 | 等待删除完成 |
| Table Not Exist | 表不存在 | 检查表名称 |

### Modify Table

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Invalid Memory Reserved Value | 内存保留值为负 | 设置 >= 0 的值 |
| Column Not Exist | 字段不存在 | 检查字段名称 |

---

## 字段操作

### Add Field

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Column Already Exists | 字段已存在 | 使用不同的字段名称 |
| Invalid Vector Field | 不支持添加向量字段 | 向量字段需在建表时定义 |
| Invalid Field Type | 字段类型无效 | 使用支持的数据类型 |
| Vector Field Missing Dimension | 向量字段缺少维度 | 设置 `dimension` 参数 |
| Invalid Dimension | 向量维度无效 | 确保维度在 [2, 4096] |
| Auto Increment Not Allowed | 不能添加自增字段 | 新字段不能设置为自增 |

---

## 索引操作

### Create Index

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Missing MetricType | 向量索引缺少 metricType | 设置 `metric_type` (IP/L2/COSINE) |
| Invalid MetricType | metricType 不支持 | 使用 IP、L2 或 COSINE |
| Incomplete HNSW Params | HNSW 参数不完整 | 设置 `m` 和 `efconstruction` |
| Invalid M Value | HNSW M 参数无效 | M 必须在有效范围内 |
| Invalid efConstruction | HNSW efConstruction 无效 | efConstruction 必须在有效范围内 |
| Target Field Has Index | 字段已存在索引 | 不能在同一字段创建多个索引 |
| Index Type Not Supported | 索引类型不支持 | 检查索引类型名称 |
| Index Field Not Exist | 索引字段不存在 | 检查字段名称 |

### Modify Index

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Vector Index Not Exist | 向量索引不存在 | 检查索引名称 |
| Invalid Auto Build Policy | 自动构建策略参数错误 | 检查策略参数 |
| Invalid Policy Type | 策略类型无效 | 使用 timing/periodical/row_count_increment |

### Rebuild Index

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Index Not Exist | 索引不存在 | 检查索引名称 |
| Index Not Dense Vector | 非稠密向量索引 | 稠密向量索引才支持重建 |
| Table State Invalid | 表状态异常 | 等待表创建/删除完成 |

### Drop Index

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Index State Invalid | 索引正在构建中 | 等待构建完成 |
| Inverted Index Not Supported | 不支持删除倒排索引 | 倒排索引不能删除 |

---

## 数据操作

### Insert/Upsert Row

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Table Not Found | 表不存在 | 检查表名称 |
| Table State Invalid | 表状态异常 | 等待表创建/删除完成 |
| Write Operations Denied | 写操作被禁用 | 联系管理员 |
| Rate Limit Exceeded | 超过请求频率限制 | 降低请求频率 |
| Too Many Rows | 单次请求行数过多 | 减少单次插入数量 |
| Rows Empty | rows 数组为空 | 提供至少一行数据 |

### Update Row

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| PrimaryKey Not Set | 未指定主键 | 提供 `primary_key` 参数 |
| PrimaryKey Empty | 主键为空对象 | 提供有效的主键值 |

### Delete Row

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Missing PrimaryKey And Filter | 未指定删除条件 | 提供 primaryKey 或 filter |
| Both PrimaryKey And Filter Exist | 同时指定了两者 | 二选一 |

### Select Row

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Invalid Limit Type | limit 参数类型无效 | limit 必须是正整数 |
| Read Operations Denied | 读操作被禁用 | 联系管理员 |
| Read Consistency Invalid | readConsistency 参数无效 | 使用 BOUNDED 或 EVENTUAL |

### Query Row

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Keys Parameter Invalid | 批量查询 keys 参数错误 | keys 必须是数组格式 |
| RetrieveVector Invalid | retrieveVector 参数无效 | 必须是布尔值 |

---

## 检索操作

### Search

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| Parameter Missing | 必需参数缺失 | 检查 table、vector 等必需参数 |
| Vector Dimension Mismatch | 向量维度不匹配 | 确保查询向量维度与索引一致 |
| Metric Type Mismatch | 度量类型不匹配 | 检查 metricType 配置 |
| Filter Syntax Invalid | 过滤条件语法错误 | 检查 filter 语法 |
| Invalid Pagination | 分页参数无效 | 检查 offset 和 limit |
| Output Fields Invalid | 返回字段配置错误 | 检查字段名称是否正确 |
| Search Timeout | 搜索超时 | 优化索引或增加超时时间 |
| Table State Invalid | 表状态异常 | 等待表创建/删除完成 |

### Multi-Vector Search

| 错误码 | 错误信息 | 解决方案 |
|--------|----------|----------|
| SubSearch Count Invalid | 子搜索数量超出范围 | 数量必须在 [2, 10] |
| Ranking Strategy Invalid | 排名策略参数错误 | 检查 strategy 参数 |
| RRF Strategy Invalid | RRF 策略参数错误 | 检查 k 参数 |
| Weights Invalid | 权重参数错误 | 权重必须是正浮点数 |
| BM25 Not Supported | 多向量检索不支持 BM25 | 检查搜索参数 |

---

## 通用解决方案

### 1. 检查参数格式

```python
def validate_name(name):
    if not isinstance(name, str):
        raise ValueError("名称必须是字符串")
    if not name or not name[0].isalpha():
        raise ValueError("名称必须以字母开头")
```

### 2. 检查权限

```python
from pymochow.exception import ServerError

try:
    client.create_table(...)
except ServerError as e:
    if "Privilege Denied" in str(e):
        print("请联系管理员授予权限")
```

### 3. 检查向量维度

```python
def validate_vector(vector, expected_dim):
    if len(vector) != expected_dim:
        raise ValueError(f"向量维度错误: 期望 {expected_dim}, 实际 {len(vector)}")
```

### 4. 检查表状态

```python
table_info = db.describe_table("my_table")
print(f"表状态: {table_info.state}")
```

### 5. 启用详细日志

```python
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("pymochow")
```

---

## 异常处理示例

```python
from pymochow.exception import ClientError, ServerError
from pymochow.model.enum import ServerErrCode

try:
    # 执行操作
    result = table.vector_search(request=request)
except ClientError as e:
    # 客户端错误（参数错误、网络问题等）
    print(f"客户端错误: {e}")
except ServerError as e:
    # 服务端错误（带错误码）
    if e.code == ServerErrCode.TABLE_NOT_EXIST:
        print("表不存在")
    elif e.code == ServerErrCode.INDEX_NOT_EXIST:
        print("索引不存在")
    else:
        print(f"服务错误 [{e.code}]: {e.message}")
```

---

**相关文档**:
- [客户端配置](../api-reference/client.md)
- [官方文档](https://cloud.baidu.com/doc/VDB/index.html)
