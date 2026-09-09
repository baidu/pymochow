# Copyright 2023 Baidu, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

"""
Examples for mochow client
"""

import time
import json
import random

import pymochow
import logging
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
    HNSWPQParams,
    HNSWSQParams,
    HNSWRABITQParams,
    PUCKParams,
    DISKANNParams,
    IVFParams,
    IVFSQParams,
    IVFPQParams,
    IVFRABITQParams,
    AutoBuildTiming,
    InvertedIndex,
    InvertedIndexParams,
    StopWordsParams,
    PersistentBitmapIndex,
    PersistentAggregatedBitmapIndex,
    RRFRank,
    WeightedRank,
)
from pymochow.model.enum import (
    FieldType, ElementType, IndexType, InvertedIndexAnalyzer, InvertedIndexParseMode, MetricType, ServerErrCode,
    InvertedIndexFieldAttribute, IndexStructureType
)
from pymochow.model.enum import TableState, IndexState, StopWordsMode
from pymochow.model.table import (
    Partition,
    Row,
    FloatVector,
    BinaryVector,
    SparseFloatVector,
    BatchQueryKey,
    AdvancedOptions,
    VectorSearchConfig,
    VectorTopkSearchRequest,
    VectorRangeSearchRequest,
    VectorBatchSearchRequest,
    MultiVectorSearchRequest,
    BM25SearchRequest,
    HybridSearchRequest,
    Highlight,
    HighlightField,
    DecayRanker,
)

logging.basicConfig(filename='example.log', level=logging.DEBUG,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestMochow:
    def __init__(self, config, vector_index_type):
        """
        init mochow client
        """
        self._client = pymochow.MochowClient(config)
        self._vector_index_type = vector_index_type

    def _build_vector_index(self, auto_build=False, metric_type=None):
        default_metric_type = metric_type or MetricType.L2
        hnswsq_metric_type = metric_type or MetricType.IP
        if self._vector_index_type == IndexType.HNSW:
            return VectorIndex(index_name="vector_idx", index_type=IndexType.HNSW,
                               field="vector", metric_type=default_metric_type,
                               params=HNSWParams(m=32, efconstruction=200),
                               auto_build=auto_build)
        elif self._vector_index_type == IndexType.HNSWPQ:
            return VectorIndex(index_name="vector_idx", index_type=IndexType.HNSWPQ,
                               field="vector", metric_type=default_metric_type,
                               params=HNSWPQParams(m=16, efconstruction=200, NSQ=4, samplerate=1.0),
                               auto_build=auto_build)
        elif self._vector_index_type == IndexType.PUCK:
            return VectorIndex(index_name="vector_idx", index_type=IndexType.PUCK,
                               field="vector", metric_type=default_metric_type,
                               params=PUCKParams(coarseClusterCount=5, fineClusterCount=5),
                               auto_build=auto_build)
        elif self._vector_index_type == IndexType.DISKANN:
            return VectorIndex(index_name="vector_idx", index_type=IndexType.DISKANN,
                               field="vector", metric_type=default_metric_type,
                               params=DISKANNParams(NSQ=4, R=64, L=100),
                               auto_build=auto_build)
        elif self._vector_index_type == IndexType.HNSWSQ:
            return VectorIndex(index_name="vector_idx", index_type=IndexType.HNSWSQ,
                               field="vector", metric_type=hnswsq_metric_type,
                               params=HNSWSQParams(m=16, efconstruction=200, qtBits=8),
                               auto_build=auto_build)
        elif self._vector_index_type == IndexType.IVF:
            return VectorIndex(index_name="vector_idx", index_type=IndexType.IVF,
                               field="vector", metric_type=default_metric_type,
                               params=IVFParams(nlist=16),
                               auto_build=auto_build)
        elif self._vector_index_type == IndexType.IVFSQ:
            return VectorIndex(index_name="vector_idx", index_type=IndexType.IVFSQ,
                               field="vector", metric_type=default_metric_type,
                               params=IVFSQParams(nlist=16, qtBits=4),
                               auto_build=auto_build)
        elif self._vector_index_type == IndexType.IVFPQ:
            return VectorIndex(index_name="vector_idx", index_type=IndexType.IVFPQ,
                               field="vector", metric_type=default_metric_type,
                               params=IVFPQParams(nlist=16, NSQ=4),
                               auto_build=auto_build)
        elif self._vector_index_type == IndexType.IVFRABITQ:
            return VectorIndex(index_name="vector_idx", index_type=IndexType.IVFRABITQ,
                               field="vector", metric_type=default_metric_type,
                               params=IVFRABITQParams(nlist=16),
                               auto_build=auto_build)
        elif self._vector_index_type == IndexType.HNSWRABITQ:
            return VectorIndex(index_name="vector_idx", index_type=IndexType.HNSWRABITQ,
                               field="vector", metric_type=default_metric_type,
                               params=HNSWRABITQParams(m=32, efconstruction=200),
                               auto_build=auto_build)
        elif self._vector_index_type == IndexType.FLAT:
            return VectorIndex(index_name="vector_idx", index_type=IndexType.FLAT,
                               field="vector", metric_type=default_metric_type,
                               auto_build=auto_build)
        else:
            raise Exception("not support index type")

    def _build_inverted_index(self):
        return InvertedIndex(
            index_name="book_segment_inverted_idx",
            fields=["segment"],
            params=InvertedIndexParams(analyzer=InvertedIndexAnalyzer.CHINESE_ANALYZER,
                                       parse_mode=InvertedIndexParseMode.COARSE_MODE,
                                       case_sensitive=True,
                                       stop_words=StopWordsParams(
                                           mode=StopWordsMode.CUSTOM,
                                           words=["呀", "啊", "哦"])),
            field_attributes=[InvertedIndexFieldAttribute.ANALYZED])

    def _vector_search_config(self):
        if self._vector_index_type == IndexType.HNSW:
            return VectorSearchConfig(ef=200, pruning=True)
        elif self._vector_index_type in (IndexType.HNSWPQ, IndexType.HNSWSQ, IndexType.HNSWRABITQ):
            return VectorSearchConfig(ef=200)
        elif self._vector_index_type == IndexType.PUCK:
            return VectorSearchConfig(search_coarse_count=5)
        elif self._vector_index_type == IndexType.DISKANN:
            return VectorSearchConfig(w=1, search_l=100)
        elif self._vector_index_type in (
                IndexType.IVF, IndexType.IVFSQ, IndexType.IVFPQ,
                IndexType.IVFRABITQ):
            return VectorSearchConfig(nprobe=5)
        elif self._vector_index_type == IndexType.FLAT:
            return VectorSearchConfig()
        else:
            raise Exception("not support index type")
    
    def clear(self):
        db = None
        try:
            db = self._client.database('book', config={'request_id': 'test_request_id_database'})
        except ClientError as e:
            logger.debug("database {} not found.".format('book'))
            pass
        
        if db is not None:
            try:
                db.drop_table('book_segments')
            except ServerError as e:
                logger.debug("drop table error {}".format(e))
                if e.code == ServerErrCode.TABLE_NOT_EXIST:
                    pass
            time.sleep(10)
            db.drop_database(config={"request_id":  "test_request_id_drop_database"})

    def create_db_and_table(self):
        """create database&table"""
        database = 'book'
        table_name = 'book_segments'

        db = self._client.create_database(database, config={"request_id": "test_request_id_create_database"})

        database_list = self._client.list_databases(config={"request_id": "test_request_id_list_database"})
        for db_item in database_list:
            logger.debug("database: {}".format(db_item.database_name))
        
        fields = []
        fields.append(Field("id", FieldType.STRING, primary_key=True,
            partition_key=True, auto_increment=False, not_null=True))
        fields.append(Field("bookName", FieldType.STRING, not_null=True))
        fields.append(Field("author", FieldType.STRING))
        fields.append(Field("page", FieldType.UINT32))
        fields.append(Field("segment", FieldType.TEXT))
        fields.append(Field("vector", FieldType.FLOAT_VECTOR, not_null=True, dimension=4))
        # 'element_type' must be set for ARRAY, 'max_capacity' is optional
        fields.append(Field("arr_field", FieldType.ARRAY, element_type=ElementType.STRING, not_null=True))
        fields.append(Field("json_field", FieldType.JSON))
        indexes = []

        indexes.append(self._build_vector_index())

        indexes.append(SecondaryIndex(index_name="book_name_idx", field="bookName"))
        indexes.append(FilteringIndex(index_name="book_name_filtering_idx",
                                       fields=[{"field": "bookName"},
                                               {"field": "page",
                                                "indexStructureType": IndexStructureType.AGGREGATED_BITMAP.value}]))
        indexes.append(FilteringIndex(index_name="arr_field_filtering_idx",
                                       fields=[{"field": "arr_field"}]))
        db.create_table(
            table_name=table_name,
            replication=3,
            partition=Partition(partition_num=1),
            schema=Schema(fields=fields, indexes=indexes),
            config={"request_id": "test_request_id_create_table"}
        )

        while True:
            time.sleep(2)
            table = db.describe_table(table_name)
            if table.state == TableState.NORMAL:
                break
        
        time.sleep(10)
        logger.debug("table: {}".format(table.to_dict()))

        db.modify_table(table_name, datanode_memory_reserved_in_gb=0.1,
                        config={"request_id": "test_request_id_modify_table"})
        time.sleep(10)
        logger.debug("table: {}".format(table.to_dict()))

    def upsert_data(self):
        """upsert data"""
        db = self._client.database('book')
        table = db.table('book_segments')

        rows = [
            Row(id='0001',
                vector=[1, 0.21, 0.213, 0],
                bookName='西游记',
                author='吴承恩',
                page=21,
                arr_field=["monkey", "journey", "classic"],
                json_field={},
                segment='富贵功名，前缘分定，为人切莫欺心。'),
            Row(id='0002',
                vector=[2, 0.22, 0.213, 0],
                bookName='西游记',
                author='吴承恩',
                page=22,
                arr_field=["dragon", "classic"],
                json_field={"page": 22},
                segment='正大光明，忠良善果弥深。些些狂妄天加谴，眼前不遇待时临。'),
            Row(id='0003',
                vector=[3, 0.23, 0.213, 0],
                bookName='三国演义',
                author='罗贯中',
                page=23,
                arr_field=["细作", "吕布"],
                json_field={"author": "罗贯中"},
                segment='细作探知这个消息，飞报吕布。'),
            Row(id='0004',
                vector=[4, 0.24, 0.213, 0],
                bookName='三国演义',
                author='罗贯中',
                page=24,
                arr_field=["吕布", "陈宫", "刘玄德"],
                json_field={"bookName": "三国演义"},
                segment='布大惊，与陈宫商议。宫曰：“闻刘玄德新领徐州，可往投之。”' \
                        '布从其言，竟投徐州来。有人报知玄德。'),
            Row(id='0005',
                vector=[5, 0.25, 0.213, 0],
                bookName='三国演义',
                author='罗贯中',
                page=25,
                arr_field=["玄德", "糜竺", "吕布"],
                json_field={
                    "product_info": {
                        "category": "electronics",
                        "brand": "BrandA"
                    },
                    "price": 99.99,
                    "in_stock": True,
                    "tags": ["summer_sale", "clearance"]
                },
                segment='玄德曰：“布乃当今英勇之士，可出迎之。”' \
                '糜竺曰：“吕布乃虎狼之徒，不可收留；收则伤人矣。'),
        ]
        
        i = 6
        while i <= 100:
            rows.append(Row(id=str(i),
                vector=[i, 0.2 + i * 0.01, 0.213, 0],
                bookName='三国演义',
                author='罗贯中',
                page=25,
                arr_field=["玄德", "糜竺", "吕布"],
                segment='玄德曰：“布乃当今英勇之士，可出迎之。”' \
                '糜竺曰：“吕布乃虎狼之徒，不可收留；收则伤人矣。'))
            i += 1

        table.upsert(rows=rows, config={"request_id": "test_request_id_upsert_data"})
        time.sleep(10)

    def change_table_schema(self):
        """change table schema"""
        db = self._client.database('book')
        table = db.table('book_segments')
        fields = []
        fields.append(Field("publisher", FieldType.STRING))
        fields.append(Field("synopsis", FieldType.STRING))

        res = table.add_fields(schema=Schema(fields=fields), 
                               config={"request_id": "test_request_id_change_table_schema"})
        logger.debug("res: {}".format(res))

    def show_table_stats(self):
        """show table stats"""
        db = self._client.database('book')
        table = db.table('book_segments')

        res = table.stats(config={"request_id": "test_request_id_show_table_stats"})
        logger.debug("res: {}".format(res))

    def query_data(self):
        """query data"""
        db = self._client.database('book')
        table = db.table('book_segments')

        primary_key = {'id':'0001'}
        projections = ["id", "bookName"]
        res = table.query(primary_key=primary_key, projections=projections, 
                retrieve_vector=True,
                vector_index_membership="vector_idx",
                config={"request_id": "test_request_id_query_data"})
        logger.debug("query res: {}".format(res))

        missing = table.query(
            primary_key={"id": "missing-vector-index-membership"},
            vector_index_membership="vector_idx",
            config={"request_id": "test_request_id_query_missing_membership"})
        logger.debug("missing membership query res: {}".format(missing))

    def batch_query_data(self):
        """query data"""
        db = self._client.database('book')
        table = db.table('book_segments')
        projections = ["id", "bookName"]
        res = table.batch_query(keys=[BatchQueryKey({'id': '0001'}),
                                      BatchQueryKey({'id': '0002'}),
                                      BatchQueryKey({'id': '1003'})],
                                projections=projections,
                                retrieve_vector=True,
                                config={"request_id": "test_request_id_batch_query_data"})
        logger.debug("batch query res: {}".format(res))

    def vector_search(self):
        """search data"""
        db = self._client.database('book')
        table = db.table('book_segments')

        table.rebuild_index("vector_idx", config={"request_id": "test_request_id_rebuild_index"})
        # while True:
        #     time.sleep(2)
        #     index = table.describe_index("vector_idx")
        #     if index.state == IndexState.NORMAL:
        #         break

        search_config = self._vector_search_config()
        limit = 5 if self._vector_index_type == IndexType.PUCK else 10

        # single topk search
        request = VectorTopkSearchRequest(vector_field="vector", vector=FloatVector([1, 0.21, 0.213, 0]),
                                          limit=limit, filter="bookName='三国演义'",
                                          config=search_config)
        res = table.vector_search(request=request, config={"request_id": "test_request_id_vector_topk_search"})
        logger.debug("topk search res: {}".format(res))

        # array field filtering index search
        request = VectorTopkSearchRequest(vector_field="vector", vector=FloatVector([1, 0.21, 0.213, 0]),
                                          limit=10, filter="array_contains(arr_field, 'classic')",
                                          config=VectorSearchConfig(ef=200))
        res = table.vector_search(request=request,
                                  config={"request_id": "test_request_id_array_contains_search"})
        logger.debug("array contains search res: {}".format(res))

        # single range search
        request = VectorRangeSearchRequest(vector_field="vector", vector=FloatVector([1, 0.21, 0.213, 0]),
                                           distance_range=(0, 20), limit=limit,
                                           filter="bookName='三国演义'",
                                           config=search_config)
        res = table.vector_search(request=request, config={"request_id": "test_request_id_vector_range_search"})
        logger.debug("range search res: {}".format(res))

        # batch search
        request = VectorBatchSearchRequest(vector_field="vector",
                                           vectors=[FloatVector([1, 0.21, 0.213, 0]),
                                                    FloatVector([1, 0.32, 0.513, 0])],
                                           limit=limit, filter="bookName='三国演义'",
                                           config=search_config)
        res = table.vector_search(request=request, config={"request_id": "test_request_id_vector_batch_search"})
        logger.debug("batch search res: {}".format(res))

        # in real world senario, you should use vectors in different vector fields
        requests = [
            VectorTopkSearchRequest(vector_field="vector",
                                    vector=FloatVector([1, 0.21, 0.213, 0]),
                                    limit=10,
                                    config=search_config),
            VectorTopkSearchRequest(vector_field="vector",
                                    vector=FloatVector([1, 0.21, 0.213, 0]),
                                    limit=10,
                                    config=search_config)
        ]
        request = MultiVectorSearchRequest(requests=requests,
                                           ranking=RRFRank(60),
                                           limit=10, filter="bookName='三国演义'")

        res = table.vector_search(request=request, projections=["id"],
                                  config={"request_id": "test_request_id_multi_vector_search"})
        logger.debug("multi vector search res: {}".format(res))

    def search_iterator(self):
        """search iterator"""
        db = self._client.database('book')
        table = db.table('book_segments')

        # Example1: search iterator for TopK search.
        logger.debug("Start search iterator for TopK search")
        request = VectorTopkSearchRequest(vector_field="vector", vector=FloatVector([1, 0.21, 0.213, 0]),
                                          limit=1000, config=VectorSearchConfig(ef=2000))

        iterator1 = table.search_iterator(request=request, batch_size=1000, total_size=10000)
        while True:
            rows = iterator1.next()
            if not rows:
                break
            logger.debug("rows:{}".format(rows))
        iterator1.close()
        logger.debug("Finish search iterator for TopK search")

        # Example2: search iterator for multi-vector search.
        logger.debug("Start search iterator for multi-vector search")
        requests = [
            VectorTopkSearchRequest(vector_field="vector",
                                    vector=FloatVector([1, 0.21, 0.213, 0]),
                                    limit=1000,
                                    config=VectorSearchConfig(ef=2000)),
            VectorTopkSearchRequest(vector_field="vector",
                                    vector=FloatVector([1, 0.21, 0.213, 0]),
                                    limit=1000,
                                    config=VectorSearchConfig(ef=2000))
        ]
        request = MultiVectorSearchRequest(requests=requests,
                                           ranking=WeightedRank([1.0, 1.0]),
                                           limit=1000)

        iterator2 = table.search_iterator(request=request, batch_size=1000, total_size=10000)
        while True:
            rows = iterator2.next()
            if not rows:
                break
            logger.debug("rows:{}".format(rows))
        iterator2.close()
        logger.debug("Finish search iterator for multi-vector search")

        bm25_request = BM25SearchRequest(
            index_name="book_segment_inverted_idx",
            search_text="吕布",
            limit=2)
        iterator3 = table.search_iterator(
            request=bm25_request, batch_size=2, total_size=4)
        while True:
            rows = iterator3.next()
            if not rows:
                break
            logger.debug("BM25 iterator rows:%s", rows)
        iterator3.close()

        hybrid_request = HybridSearchRequest(
            vector_request=VectorTopkSearchRequest(
                vector_field="vector",
                vector=FloatVector([1, 0.21, 0.213, 0]),
                limit=2,
                config=VectorSearchConfig(ef=200)),
            bm25_request=BM25SearchRequest(
                index_name="book_segment_inverted_idx", search_text="吕布"),
            vector_weight=0.4, bm25_weight=0.6, limit=2)
        iterator4 = table.search_iterator(
            request=hybrid_request, batch_size=2, total_size=4)
        while True:
            rows = iterator4.next()
            if not rows:
                break
            logger.debug("Hybrid iterator rows:%s", rows)
        iterator4.close()

    def bm25_search(self):
        """bm25 search"""
        db = self._client.database('book')
        table = db.table('book_segments')

        request = BM25SearchRequest(index_name="book_segment_inverted_idx",
                                    search_text="potato",
                                    limit=10,
                                    filter="bookName='三国演义'",
                                    synonyms=[["potato", "spud"]],
                                    highlight=Highlight(
                                        fields={"segment": HighlightField(
                                            number_of_fragments=0)},
                                        pre_tags=["<em>"],
                                        post_tags=["</em>"]),
                                    decay=[DecayRanker(
                                        "LINEAR", "page", 25, 2)])
        res = table.bm25_search(request=request, config={"request_id":  "test_request_id_bm25_search"})
        logger.debug("BM25 search res: {}".format(res))

    def hybrid_search(self):
        """do anns and/or bm25 search"""
        db = self._client.database('book')
        table = db.table('book_segments')

        config = self._vector_search_config()
        vector_request = VectorTopkSearchRequest(vector_field="vector",
                                                 vector=FloatVector([1, 0.21, 0.213, 0]),
                                                 limit=10,
                                                 config=config)
        bm25_request = BM25SearchRequest(index_name="book_segment_inverted_idx",
                                         search_text="吕布")
        hybrid_request = HybridSearchRequest(vector_request=vector_request,
                                             vector_weight=0.4,
                                             bm25_request=bm25_request,
                                             bm25_weight=0.6,
                                             filter="bookName='三国演义'",
                                             limit=15,
                                             decay=[DecayRanker(
                                                 "LINEAR", "page", 25, 2)])

        res = table.hybrid_search(request=hybrid_request, config={"request_id":  "test_request_id_hybrid_search"})

        logger.debug("hybrid search res: {}".format(res))

    def two_phase_retrieval_search(self):
        """demonstrate two-phase retrieval for vector search and hybrid search

        Two-phase retrieval reduces IO and network transfer when projection is large
        and shard count is high:
          Phase 1 - fetch primary keys only from each shard, merge global TopK.
          Phase 2 - fetch full rows for TopK primary keys only.

        Constraints:
          - Not supported for MultiVectorSearch.
          - Not compatible with decay configuration.
        """
        db = self._client.database('book')
        table = db.table('book_segments')

        config = self._vector_search_config()

        advanced_options = AdvancedOptions(two_phase_retrieval=True)

        # TopK vector search with two-phase retrieval
        request = VectorTopkSearchRequest(
            vector_field="vector",
            vector=FloatVector([1, 0.21, 0.213, 0]),
            limit=10,
            filter="bookName='三国演义'",
            config=config,
            advanced_options=advanced_options)
        res = table.vector_search(
            request=request,
            config={"request_id": "test_request_id_two_phase_topk"})
        logger.debug("two-phase retrieval TopK search res: {}".format(res))

        # Hybrid search with two-phase retrieval
        vector_request = VectorTopkSearchRequest(
            vector_field="vector",
            vector=FloatVector([1, 0.21, 0.213, 0]),
            limit=10,
            config=config)
        bm25_request = BM25SearchRequest(
            index_name="book_segment_inverted_idx",
            search_text="吕布")
        hybrid_request = HybridSearchRequest(
            vector_request=vector_request,
            vector_weight=0.4,
            bm25_request=bm25_request,
            bm25_weight=0.6,
            filter="bookName='三国演义'",
            limit=15,
            advanced_options=advanced_options)
        res = table.hybrid_search(
            request=hybrid_request,
            config={"request_id": "test_request_id_two_phase_hybrid"})
        logger.debug("two-phase retrieval hybrid search res: {}".format(res))

    def select_data(self):
        """select data"""
        db = self._client.database('book')
        table = db.table('book_segments')
        projections = ["id", "bookName", "json_field"]

        select_finished = False
        marker = None
        while True:
            res = table.select(marker=marker, projections=projections, limit=20, 
                               config={"request_id":  "test_request_id_select"})
            logger.debug("res: {}".format(res))
            if res.is_truncated is False:
                logger.debug("select finished")
                break
            else:
                logger.debug("select next batch")
                marker = res.next_marker

    def update_data(self):
        """update data"""
        db = self._client.database('book')
        table = db.table('book_segments')

        primary_key = {'id': '0001'}
        update_fields = {'bookName': '红楼梦',
                         'author': '曹雪芹',
                         'page': 21,
                         'segment': '满纸荒唐言，一把辛酸泪'}
        res = table.update(primary_key=primary_key, update_fields=update_fields, 
                           config={"request_id":  "test_request_id_update"})
        logger.debug("res: {}".format(res))

    def delete_data(self):
        """delete data"""
        db = self._client.database('book')
        table = db.table('book_segments')

        primary_key = {'id': '0001'}
        res = table.delete(primary_key=primary_key, config={"request_id":  "test_request_id_delete"})
        logger.debug("res: {}".format(res))

    def drop_and_create_vindex(self):
        """drop and create vindex"""
        db = self._client.database('book')
        table = db.table('book_segments')
        table.drop_index("vector_idx", config={"request_id":  "test_request_id_drop_index"})
        while True:
            time.sleep(2)
            try:
                index = table.describe_index("vector_idx")
            except ServerError as e:
                logger.debug("code: {}".format(e.code))
                if e.code == ServerErrCode.INDEX_NOT_EXIST:
                    break
        
        indexes = [self._build_vector_index(auto_build=False, metric_type=MetricType.L2)]
        table.create_indexes(indexes, config={"request_id":  "test_request_id_create_index"})
        time.sleep(1)
        table.modify_index(index_name="vector_idx", auto_build=True, 
                        auto_build_index_policy=AutoBuildTiming("2024-01-01 00:00:00"))
        index = table.describe_index("vector_idx")
        logger.debug("index: {}".format(index.to_dict()))

    def create_inverted_index(self):
        """create inverted index"""
        db = self._client.database('book')
        table = db.table('book_segments')
        index_name = "book_segment_inverted_idx"

        table.create_indexes([self._build_inverted_index()],
                             config={"request_id": "test_request_id_create_iindex"})
        deadline = time.time() + 300
        while True:
            time.sleep(2)
            index = table.describe_index(index_name)
            logger.debug("iindex state: %s", index.state)
            if index.state == IndexState.NORMAL:
                logger.debug("iindex after create: {}".format(index.to_dict()))
                break
            if time.time() > deadline:
                raise RuntimeError("timeout waiting for inverted index {}".format(index_name))

    def drop_inverted_index(self):
        """drop inverted index"""
        db = self._client.database('book')
        table = db.table('book_segments')
        index_name = "book_segment_inverted_idx"

        table.drop_index(index_name, config={"request_id": "test_request_id_drop_iindex"})
        while True:
            time.sleep(2)
            try:
                table.describe_index(index_name)
            except ServerError as e:
                logger.debug("code: {}".format(e.code))
                if e.code == ServerErrCode.INDEX_NOT_EXIST:
                    break

    def delete_and_drop(self):
        """delete and drop"""
        db = self._client.database('book')

        db.drop_table('book_segments')
        time.sleep(10)

        db.drop_database()
        self._client.close()
    
    def binary_vector_usage_example(self):
        # convert num to it's binary representation, a list of 0 and 1
        def num_to_binary_list(num, dimension):
            binary_str = bin(num)[2:]
            binary_list = [int(bit) for bit in binary_str]
            if len(binary_list) > dimension:
                binary_list = binary_list[:dimension]
            elif len(binary_list) < dimension:
                tmp = [0] * (dimension - len(binary_list))
                tmp.extend(binary_list)
                binary_list = tmp
            assert(len(binary_list) == dimension)
            return binary_list

        # 1. create table
        database = "test_binary_vec"
        table_name = "test_binary_vec_tab"

        db = None
        try:
            db = self._client.database(database)
        except ClientError:
            pass
        if db is not None:
            try:
                db.drop_table(table_name)
            except ServerError:
                pass
            time.sleep(10)
            db.drop_database()

        db = self._client.create_database(database)
        vector_dimension = 128
        fields = []
        fields.append(Field("id", FieldType.STRING, primary_key=True,
                      partition_key=True, auto_increment=False, not_null=True))
        fields.append(Field("vector", FieldType.BINARY_VECTOR, not_null=True, dimension=vector_dimension))
        db.create_table(table_name=table_name, replication=3, partition=Partition(
            partition_num=1), schema=Schema(fields=fields))
        while True:
            time.sleep(2)
            if db.describe_table(table_name).state == TableState.NORMAL:
                break

        # 2. insert to table
        table = db.table(table_name)
        rows = []
        for num in range(50):
            vec = BinaryVector.from_binary_list(num_to_binary_list(num, vector_dimension))
            rows.append(Row(id=str(num), vector=vec))
        table.upsert(rows=rows)

        # 3. search binary vector
        target = BinaryVector.from_binary_list(num_to_binary_list(123, vector_dimension))
        request = VectorTopkSearchRequest(vector_field="vector", vector=target, limit=10)
        res = table.vector_search(request=request)
        logger.debug("res: {}".format(res))

    def sparse_vector_usage_example(self):
        # 1. create table
        database = "test_sparse_vec"
        table_name = "test_sparse_vec_tab"

        db = None
        try:
            db = self._client.database(database)
        except ClientError:
            pass
        if db is not None:
            try:
                db.drop_table(table_name)
            except ServerError:
                pass
            time.sleep(10)
            db.drop_database()

        db = self._client.create_database(database)
        fields = []
        fields.append(Field("id", FieldType.STRING, primary_key=True,
                            partition_key=True, auto_increment=False, not_null=True))
        fields.append(Field("vector", FieldType.SPARSE_FLOAT_VECTOR, not_null=True))

        indexes = []
        indexes.append(VectorIndex(index_name="sparse_vector_idx",
                                   index_type=IndexType.SPARSE_OPTIMIZED_FLAT,
                                   field="vector",
                                   metric_type=MetricType.IP))

        db.create_table(table_name=table_name, replication=3,
                        partition=Partition(partition_num=1),
                        schema=Schema(fields=fields, indexes=indexes))
        while True:
            time.sleep(2)
            if db.describe_table(table_name).state == TableState.NORMAL:
                break

        # 2. insert to table
        rows = []
        table = db.table(table_name)
        for num in range(50):
            vec = SparseFloatVector.from_dict({1: 0.56465, 100: 0.2366456, 10000: 0.543111})
            rows.append(Row(id=str(num), vector=vec))
        table.upsert(rows=rows)

        # 3. search binary vector
        target = SparseFloatVector.from_dict({1: 0.56465, 100: 0.2366456, 10000: 0.543111})
        request = VectorTopkSearchRequest(vector_field="vector", vector=target, limit=10)
        res = table.vector_search(request=request)
        logger.debug("res: {}".format(res))

    def alias_and_unalias(self):
        """alias and unalias"""
        db = self._client.database('book')
        table = db.table('book_segments')
        table_alias = 'book_segments_alias'
        table.alias(table_alias)
        table = db.table('book_segments')
        logger.debug("table {}".format(table.to_dict()))
        table.unalias(table_alias)
        table = db.table('book_segments')
        logger.debug("table {}".format(table.to_dict()))

if __name__ == "__main__":
    account = 'root'
    api_key = '********'
    endpoint = 'http://*.*.*.*:*' #example:http://127.0.0.1:8511

    config = Configuration(credentials=BceCredentials(account, api_key),
            endpoint=endpoint)
    test_vdb = TestMochow(config, IndexType.HNSW)
    test_vdb.clear()
    test_vdb.create_db_and_table()
    test_vdb.upsert_data()
    test_vdb.select_data()
    test_vdb.change_table_schema()
    test_vdb.show_table_stats()
    test_vdb.query_data()
    test_vdb.batch_query_data()
    test_vdb.vector_search()
    test_vdb.create_inverted_index()
    test_vdb.bm25_search()
    test_vdb.hybrid_search()
    test_vdb.search_iterator()
    test_vdb.two_phase_retrieval_search()
    test_vdb.drop_inverted_index()
    test_vdb.update_data()
    test_vdb.delete_data()
    test_vdb.drop_and_create_vindex()
    test_vdb.binary_vector_usage_example()
    test_vdb.sparse_vector_usage_example()
    test_vdb.alias_and_unalias()
    test_vdb.delete_and_drop()
