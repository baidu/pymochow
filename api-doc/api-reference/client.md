# 客户端配置

## MochowClient

`MochowClient` 是 SDK 的核心入口类，用于创建和管理与 VectorDB 服务的连接。

### 初始化

```python
import pymochow
from pymochow.configuration import Configuration
from pymochow.auth.bce_credentials import BceCredentials

config = Configuration(
    credentials=BceCredentials(account, api_key),
    endpoint=endpoint
)
client = pymochow.MochowClient(config)
```

### 方法列表

| 方法 | 说明 | 返回值 |
|------|------|--------|
| `create_database(name)` | 创建数据库 | `Database` |
| `list_databases()` | 列出所有数据库 | `list[Database]` |
| `database(name)` | 获取数据库对象 | `Database` |
| `close()` | 关闭客户端连接 | `None` |

---

## Configuration

`Configuration` 类用于配置客户端参数。

### 构造参数

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `credentials` | `BceCredentials` | 是 | 认证凭证 |
| `endpoint` | `str` | 是 | 服务端点 |
| `connection_timeout_in_mills` | `int` | 否 | 连接超时（毫秒），默认 5000 |
| `send_buf_size` | `int` | 否 | 发送缓冲区大小 |
| `recv_buf_size` | `int` | 否 | 接收缓冲区大小 |

### 示例

```python
from pymochow.configuration import Configuration
from pymochow.auth.bce_credentials import BceCredentials

config = Configuration(
    credentials=BceCredentials('root', 'your_api_key'),
    endpoint='http://127.0.0.1:8511',
    connection_timeout_in_mills=10000  # 10秒超时
)
```

---

## BceCredentials

`BceCredentials` 类用于存储账户认证信息。

### 构造参数

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `account` | `str` | 是 | 账户名，通常为 `root` |
| `api_key` | `str` | 是 | API 密钥 |

### 示例

```python
from pymochow.auth.bce_credentials import BceCredentials

credentials = BceCredentials(
    account='root',
    api_key='your_api_key'
)
```

### 安全建议

> **警告**: 不要在代码中硬编码 API Key，应使用环境变量。

```python
import os
from pymochow.auth.bce_credentials import BceCredentials

credentials = BceCredentials(
    account=os.getenv('MOCHOW_ACCOUNT', 'root'),
    api_key=os.getenv('MOCHOW_API_KEY')
)
```

---

## 超时配置

### 全局超时

```python
config = Configuration(
    credentials=credentials,
    endpoint=endpoint,
    connection_timeout_in_mills=30000  # 30秒
)
```

### 最佳实践

| 操作类型 | 建议超时 |
|----------|----------|
| 简单查询 | 5-10秒 |
| 向量检索 | 10-30秒 |
| 批量插入 | 30-60秒 |
| 索引重建 | 60秒以上 |

---

## 异常处理

SDK 定义了两种主要异常类型：

### ClientError

客户端错误，如参数错误、网络问题等。

```python
from pymochow.exception import ClientError

try:
    db = client.database('non_existent_db')
except ClientError as e:
    print(f"客户端错误: {e}")
```

### ServerError

服务端错误，包含错误码。

```python
from pymochow.exception import ServerError
from pymochow.model.enum import ServerErrCode

try:
    client.create_database('existing_db')
except ServerError as e:
    if e.code == ServerErrCode.DATABASE_ALREADY_EXIST:
        print("数据库已存在")
    else:
        print(f"服务错误: {e.code} - {e.message}")
```

---

## 日志配置

启用 SDK 日志以便调试：

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pymochow')
```

---

**相关文档**:
- [数据库操作](./database.md)
- [错误码参考](../troubleshooting/error-codes.md)
