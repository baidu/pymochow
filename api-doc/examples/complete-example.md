# 完整示例

## 概述

本文档提供一个从头到尾的完整示例，涵盖数据库创建、表定义、数据插入和向量检索的全流程。

---

## 完整代码

```python
"""
Baidu VectorDB Python SDK 完整示例
演示从建库到检索的全流程
"""

import time
import pymochow
from pymochow.configuration import Configuration
from pymochow.auth.bce_credentials import BceCredentials
from pymochow.exception import ClientError, ServerError
from pymochow.model.schema import (
    Schema,
    Field,
    SecondaryIndex,
    FilteringIndex,
    VectorIndex,
    HNSWParams,
    InvertedIndex,
    InvertedIndexParams,
    RRFRank,
)
from pymochow.model.enum import (
    FieldType,
    ElementType,
    IndexType,
    MetricType,
    TableState,
    IndexState,
    InvertedIndexAnalyzer,
    InvertedIndexParseMode,
    InvertedIndexFieldAttribute,
    ServerErrCode,
)
from pymochow.model.table import (
    Partition,
    Row,
    FloatVector,
    BatchQueryKey,
    VectorSearchConfig,
    VectorTopkSearchRequest,
    VectorRangeSearchRequest,
    VectorBatchSearchRequest,
    MultiVectorSearchRequest,
    BM25SearchRequest,
    HybridSearchRequest,
)


class VectorDBExample:
    """VectorDB 完整示例类"""
    
    def __init__(self, account, api_key, endpoint):
        """初始化客户端"""
        config = Configuration(
            credentials=BceCredentials(account, api_key),
            endpoint=endpoint
        )
        self._client = pymochow.MochowClient(config)
    
    def cleanup(self, database_name, table_name):
        """清理已存在的资源"""
        try:
            db = self._client.database(database_name)
            try:
                db.drop_table(table_name)
                time.sleep(10)
            except ServerError:
                pass
            db.drop_database()
        except ClientError:
            pass
    
    def create_database_and_table(self, database_name, table_name):
        """创建数据库和表"""
        # 创建数据库
        db = self._client.create_database(database_name)
        print(f"数据库 '{database_name}' 创建成功")
        
        # 定义字段
        fields = [
            Field("id", FieldType.STRING, primary_key=True, 
                  partition_key=True, not_null=True),
            Field("bookName", FieldType.STRING, not_null=True),
            Field("author", FieldType.STRING),
            Field("page", FieldType.UINT32),
            Field("segment", FieldType.TEXT),
            Field("vector", FieldType.FLOAT_VECTOR, not_null=True, dimension=4),
            Field("tags", FieldType.ARRAY, element_type=ElementType.STRING, not_null=True),
            Field("metadata", FieldType.JSON),
        ]
        
        # 定义索引
        indexes = [
            # 向量索引
            VectorIndex(
                index_name="vector_idx",
                index_type=IndexType.HNSW,
                field="vector",
                metric_type=MetricType.L2,
                params=HNSWParams(m=32, efconstruction=200)
            ),
            # 二级索引
            SecondaryIndex(index_name="book_name_idx", field="bookName"),
            # 过滤索引
            FilteringIndex(index_name="filter_idx", fields=["bookName", "author"]),
            # 倒排索引
            InvertedIndex(
                index_name="segment_inverted_idx",
                fields=["segment"],
                params=InvertedIndexParams(
                    analyzer=InvertedIndexAnalyzer.CHINESE_ANALYZER,
                    parse_mode=InvertedIndexParseMode.COARSE_MODE,
                    case_sensitive=True
                ),
                field_attributes=[InvertedIndexFieldAttribute.ANALYZED]
            ),
        ]
        
        # 创建表
        db.create_table(
            table_name=table_name,
            replication=3,
            partition=Partition(partition_num=1),
            schema=Schema(fields=fields, indexes=indexes)
        )
        
        # 等待表就绪
        while True:
            time.sleep(2)
            table = db.describe_table(table_name)
            if table.state == TableState.NORMAL:
                break
        
        print(f"表 '{table_name}' 创建成功")
        return db
    
    def insert_data(self, db, table_name):
        """插入数据"""
        table = db.table(table_name)
        
        rows = [
            Row(
                id='0001',
                vector=[1, 0.21, 0.213, 0],
                bookName='西游记',
                author='吴承恩',
                page=21,
                tags=[],
                metadata={},
                segment='富贵功名，前缘分定，为人切莫欺心。'
            ),
            Row(
                id='0002',
                vector=[2, 0.22, 0.213, 0],
                bookName='西游记',
                author='吴承恩',
                page=22,
                tags=['经典'],
                metadata={'page': 22},
                segment='正大光明，忠良善果弥深。'
            ),
            Row(
                id='0003',
                vector=[3, 0.23, 0.213, 0],
                bookName='三国演义',
                author='罗贯中',
                page=23,
                tags=['细作', '吕布'],
                metadata={'author': '罗贯中'},
                segment='细作探知这个消息，飞报吕布。'
            ),
            Row(
                id='0004',
                vector=[4, 0.24, 0.213, 0],
                bookName='三国演义',
                author='罗贯中',
                page=24,
                tags=['吕布', '陈宫', '刘玄德'],
                metadata={'bookName': '三国演义'},
                segment='布大惊，与陈宫商议。宫曰："闻刘玄德新领徐州，可往投之。"'
            ),
            Row(
                id='0005',
                vector=[5, 0.25, 0.213, 0],
                bookName='三国演义',
                author='罗贯中',
                page=25,
                tags=['玄德', '糜竺', '吕布'],
                metadata={'category': 'classic'},
                segment='玄德曰："布乃当今英勇之士，可出迎之。"'
            ),
        ]
        
        table.upsert(rows=rows)
        time.sleep(2)
        print(f"插入 {len(rows)} 条数据")
        return table
    
    def query_data(self, table):
        """查询数据"""
        # 主键查询
        result = table.query(
            primary_key={'id': '0001'},
            projections=['id', 'bookName', 'author'],
            retrieve_vector=True
        )
        print(f"主键查询结果: {result}")
        
        # 批量查询
        result = table.batch_query(
            keys=[
                BatchQueryKey({'id': '0001'}),
                BatchQueryKey({'id': '0002'}),
            ],
            projections=['id', 'bookName'],
            retrieve_vector=False
        )
        print(f"批量查询结果: {result}")
        
        # 条件筛选
        result = table.select(
            projections=['id', 'bookName', 'author'],
            limit=10
        )
        print(f"条件筛选结果: {result.rows}")
    
    def vector_search(self, table):
        """向量检索"""
        # 重建索引
        table.rebuild_index("vector_idx")
        while True:
            time.sleep(2)
            index = table.describe_index("vector_idx")
            if index.state == IndexState.NORMAL:
                break
        
        # TopK 检索
        request = VectorTopkSearchRequest(
            vector_field="vector",
            vector=FloatVector([1, 0.21, 0.213, 0]),
            limit=10,
            filter="bookName='三国演义'",
            config=VectorSearchConfig(ef=200, pruning=True)
        )
        result = table.vector_search(request=request)
        print(f"TopK 检索结果: {result}")
        
        # 范围检索
        request = VectorRangeSearchRequest(
            vector_field="vector",
            vector=FloatVector([1, 0.21, 0.213, 0]),
            distance_range=(0, 20),
            limit=10,
            config=VectorSearchConfig(ef=200)
        )
        result = table.vector_search(request=request)
        print(f"范围检索结果: {result}")
        
        # 批量检索
        request = VectorBatchSearchRequest(
            vector_field="vector",
            vectors=[
                FloatVector([1, 0.21, 0.213, 0]),
                FloatVector([2, 0.22, 0.213, 0])
            ],
            limit=5,
            config=VectorSearchConfig(ef=200)
        )
        result = table.vector_search(request=request)
        print(f"批量检索结果: {result}")
        
        # 多向量检索
        sub_requests = [
            VectorTopkSearchRequest("vector", FloatVector([1, 0.21, 0.213, 0]), 10,
                                    config=VectorSearchConfig(ef=200)),
            VectorTopkSearchRequest("vector", FloatVector([2, 0.22, 0.213, 0]), 10,
                                    config=VectorSearchConfig(ef=200)),
        ]
        request = MultiVectorSearchRequest(
            requests=sub_requests,
            ranking=RRFRank(60),
            limit=10
        )
        result = table.vector_search(request=request, projections=['id'])
        print(f"多向量检索结果: {result}")
    
    def hybrid_search(self, table):
        """混合检索"""
        # BM25 检索
        bm25_request = BM25SearchRequest(
            index_name="segment_inverted_idx",
            search_text="吕布",
            limit=10,
            filter="bookName='三国演义'"
        )
        result = table.bm25_search(request=bm25_request)
        print(f"BM25 检索结果: {result}")
        
        # 混合检索
        vector_request = VectorTopkSearchRequest(
            vector_field="vector",
            vector=FloatVector([1, 0.21, 0.213, 0]),
            limit=10,
            config=VectorSearchConfig(ef=200)
        )
        hybrid_request = HybridSearchRequest(
            vector_request=vector_request,
            vector_weight=0.4,
            bm25_request=BM25SearchRequest("segment_inverted_idx", "吕布"),
            bm25_weight=0.6,
            filter="bookName='三国演义'",
            limit=15
        )
        result = table.hybrid_search(request=hybrid_request)
        print(f"混合检索结果: {result}")
    
    def update_and_delete(self, table):
        """更新和删除"""
        # 更新
        table.update(
            primary_key={'id': '0001'},
            update_fields={'bookName': '红楼梦', 'author': '曹雪芹'}
        )
        print("更新成功")
        
        # 删除
        table.delete(primary_key={'id': '0005'})
        print("删除成功")
    
    def cleanup_resources(self, db, table_name):
        """清理资源"""
        db.drop_table(table_name)
        time.sleep(10)
        db.drop_database()
        self._client.close()
        print("资源清理完成")


def main():
    """主函数"""
    # 配置
    account = 'root'
    api_key = 'your_api_key'
    endpoint = 'http://127.0.0.1:8511'
    database_name = 'book'
    table_name = 'book_segments'
    
    # 初始化
    example = VectorDBExample(account, api_key, endpoint)
    
    # 清理已有资源
    example.cleanup(database_name, table_name)
    
    # 创建数据库和表
    db = example.create_database_and_table(database_name, table_name)
    
    # 插入数据
    table = example.insert_data(db, table_name)
    
    # 查询数据
    example.query_data(table)
    
    # 向量检索
    example.vector_search(table)
    
    # 混合检索
    example.hybrid_search(table)
    
    # 更新和删除
    example.update_and_delete(table)
    
    # 清理资源
    example.cleanup_resources(db, table_name)


if __name__ == "__main__":
    main()
```

---

## 运行说明

1. 安装 SDK：`pip install pymochow`
2. 修改配置：更新 `account`、`api_key`、`endpoint`
3. 运行脚本：`python complete_example.py`

---

**相关文档**:
- [快速开始](../getting-started/quickstart.md)
- [API 参考](../api-reference/client.md)
