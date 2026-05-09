# 百度向量数据库 Python SDK 文档

欢迎使用百度向量数据库 (VectorDB/Mochow) Python SDK 文档。

## 快速链接

- [安装指南](./getting-started/installation.md)
- [快速开始](./getting-started/quickstart.md)
- [API 参考](#api-参考)
- [错误码参考](./troubleshooting/error-codes.md)

---

## 文档目录

### 快速入门

| 文档 | 说明 |
|------|------|
| [安装指南](./getting-started/installation.md) | SDK 安装与环境配置 |
| [快速开始](./getting-started/quickstart.md) | 5分钟快速上手 |

### 核心概念

| 文档 | 说明 |
|------|------|
| [数据模型](./core-concepts/data-model.md) | 字段类型、主键、分区键 |
| [索引类型](./core-concepts/index-types.md) | HNSW、HNSWPQ、DISKANN 等 |

### API 参考

| 文档 | 说明 |
|------|------|
| [客户端配置](./api-reference/client.md) | MochowClient、Configuration |
| [数据库操作](./api-reference/database.md) | 创建、删除、列出数据库 |
| [表操作](./api-reference/table.md) | 创建、修改、删除表 |
| [数据操作](./api-reference/data-operations.md) | 插入、查询、更新、删除 |
| [向量检索](./api-reference/vector-search.md) | TopK、Range、Batch 检索 |
| [索引操作](./api-reference/index-operations.md) | 创建、重建、删除索引 |

### 进阶用法

| 文档 | 说明 |
|------|------|
| [混合检索](./advanced/hybrid-search.md) | 向量 + BM25 混合检索 |
| [Binary/Sparse 向量](./advanced/binary-sparse-vector.md) | 二进制和稀疏向量 |
| [搜索迭代器](./advanced/search-iterator.md) | 分页检索 |

### 最佳实践

| 文档 | 说明 |
|------|------|
| [最佳实践指南](./best-practices/overview.md) | Schema、索引、性能优化 |

### 故障排查

| 文档 | 说明 |
|------|------|
| [错误码参考](./troubleshooting/error-codes.md) | 完整错误码及解决方案 |

### 示例

| 文档 | 说明 |
|------|------|
| [完整示例](./examples/complete-example.md) | 从建表到检索完整流程 |
| [典型场景](./examples/use-cases.md) | RAG、推荐等应用场景 |

---

## 核心类一览

| 类名 | 说明 |
|------|------|
| `MochowClient` | 客户端入口 |
| `Configuration` | 配置类 |
| `BceCredentials` | 认证凭证 |
| `Database` | 数据库操作 |
| `Table` | 表操作 |
| `Row` | 数据行 |
| `FloatVector` | 稠密向量 |
| `BinaryVector` | 二进制向量 |
| `SparseFloatVector` | 稀疏向量 |

---

## 相关资源

- [GitHub 仓库](https://github.com/baidu/mochow-python-sdk)
- [官方文档](https://cloud.baidu.com/doc/VDB/index.html)
- [PyPI 包](https://pypi.org/project/pymochow/)
