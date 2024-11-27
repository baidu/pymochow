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
Examples for ai search sdk
"""
import os
import time
import json
import random
import sys
import uuid

import pymochow
import logging
from pymochow.configuration import Configuration
from pymochow.auth.bce_credentials import BceCredentials
from pymochow.exception import ClientError, ServerError
from pymochow.model.schema import (
    Schema, 
    Field, 
    SecondaryIndex, 
    VectorIndex, 
    HNSWParams, 
    HNSWPQParams, 
    PUCKParams, 
    AutoBuildTiming,
    InvertedIndex,
    InvertedIndexParams
)
from pymochow.model.enum import (
    InvertedIndexAnalyzer, 
    InvertedIndexParseMode, 
    InvertedIndexFieldAttribute
)
from pymochow.model.enum import FieldType, IndexType, MetricType, ServerErrCode
from pymochow.model.enum import TableState, IndexState
from pymochow.model.table import (
    Partition, 
    Row, 
    AnnSearch, 
    HNSWSearchParams,
    VectorTopkSearchRequest,
    VectorRangeSearchRequest,
    VectorBatchSearchRequest,
    BM25SearchRequest,
    HybridSearchRequest,
    VectorSearchConfig
)

from pymochow.ai.dochub import (
    LocalDocumentHub,
    BosDocumentHub,
    DocumentHubEnv
)
from pymochow.ai.processor import QianfanDocProcessor, LangchainDocProcessor
from pymochow.ai.embedder import QianfanEmbedder
from pymochow.ai.pipeline import DefaultPipeline
from pymochow.model.document import Document


if sys.version_info[0] >= 3:
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)

# 配置日志记录
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('ai_search_demo.log', 'w', 'utf-8')])
logger = logging.getLogger(__name__)

class AiSearchDemo:
    """ai search demo"""
    def __init__(self, config):
        """
        init mochow client
        """
        self._client = pymochow.MochowClient(config)
    
    def manage_document_in_local(self):
        """
        Manages documents within a local document hub, including adding, listing, and removing documents.
        """
        # 设置本地文档管理的根路径, 举例local://root_path/
        env = DocumentHubEnv(root_path="local://your_root_path/")
        doc_hub = LocalDocumentHub(env=env)
        kb_id = str(uuid.uuid4())
        doc = Document(
            kb_id=kb_id, # 知识库ID
            doc_name="test/test.pdf", # 文档名，指期望文档存储在文档管理中的相对路径，举例：test/test.pdf
            file_path="./test.pdf" # 文档的本地路径
        )
        # 将文件纳入到文档管理中，将文件./test.pdf复制到/root_path/test/test.pdf
        doc_hub.add(doc=doc)
        
        docs = doc_hub.list()
        for doc in docs:
            logger.debug("doc: {} in hub".format(doc.to_dict()))

        doc_hub.remove(doc=doc)
    
    def manage_document_in_bos(self):
        """
        Manages documents within a bos document hub, including adding, listing, and removing documents.
        """
        env = DocumentHubEnv(
            endpoint="bj.bcebos.com",
            ak="your_bos_ak",
            sk="your_bos_sk",
            root_path="bos://your_bucket/object_prefix",
            local_cache_path="./tmp"  # local file cache dir
        )
        doc_hub = BosDocumentHub(env=env)
        kb_id = str(uuid.uuid4())
        doc = Document(
            kb_id=kb_id, # 知识库ID
            doc_name="test/test.pdf", # 文档名，指期望文档存储在文档管理中的相对路径，举例：test/test.pdf
            file_path="./test.pdf" # 文档的本地路径
        )
        
        doc  = doc_hub.add(doc=doc)
        logger.debug("doc:{} added into hub".format(doc.to_dict()))
        
        docs = doc_hub.list()
        for doc in docs:
            logger.debug("doc: {} in hub".format(doc.to_dict()))
        
        doc = Document(
            kb_id=kb_id,
            doc_name="test/test.pdf"
        )
        doc = doc_hub.load(doc)
        logger.debug("load doc: {} from hub".format(doc.to_dict()))

        doc_hub.remove(doc=doc)
    
    def parser_document(self):
        """
        Parses a document by processing its contents into chunks using a document processor, 
        and manages the document lifecycle in a local hub.
        """
        env = DocumentHubEnv(root_path="local://your_root_path/")
        doc_hub = LocalDocumentHub(env=env)
        kb_id = str(uuid.uuid4())
        doc = Document(
            kb_id=kb_id, # 知识库ID
            doc_name="test/test.pdf", # 文档名，指期望文档存储在文档管理中的相对路径，举例：test/test.pdf
            file_path="./test.pdf" # 文档的本地路径
        )
        doc = doc_hub.add(doc=doc)
        
        os.environ["APPBUILDER_TOKEN"] = "your_ab_token"
        doc_processor = QianfanDocProcessor()
        chunks = doc_processor.process_doc(doc=doc)
        for chunk in chunks:
            logger.info(f"document chunk:{chunk.to_dict()}")
        doc_hub.remove(doc=doc)
    
    def embedding_document(self):
        """
        Parse and split a document into chunks,
        and then performing embedding operations on these chunks.
        """
        env = DocumentHubEnv(root_path="local://your_root_path/")
        doc_hub = LocalDocumentHub(env=env)
        kb_id = str(uuid.uuid4())
        doc = Document(
            kb_id=kb_id, # 知识库ID
            doc_name="test/test.pdf", # 文档名，指期望文档存储在文档管理中的相对路径，举例：test/test.pdf
            file_path="./test.pdf" # 文档的本地路径
        )
        doc = doc_hub.add(doc=doc)
        
        os.environ["APPBUILDER_TOKEN"] = "your_ab_token"
        doc_processor = QianfanDocProcessor(maximum_page_length=300, page_overlap_length=50)
        chunks = doc_processor.process_doc(doc=doc)
        for chunk in chunks:
            logger.info(f"document chunk after process:{chunk.to_dict()}")
        embedder = QianfanEmbedder(batch=2)
        chunks = embedder.embedding(chunks)
        for chunk in chunks:
            logger.info(f"document chunk after embedding:{chunk.to_dict()}")
        doc_hub.remove(doc=doc)
    
    def create_or_get_db(self, db_name):
        """
        create or get database in vectordb
        """
        if not db_name:
            raise ValueError("Database name cannot be None or empty.")

        try:
            db = self._client.database(db_name)
            return db
        except ClientError as e:
            # The exception is handled by attempting to create the database
            pass

        db = self._client.create_database(db_name)
        return db
    
    def create_or_get_table(
        self, 
        db, 
        table_name, 
        schema,
        replication=1,
        partition=Partition(partition_num=1)
    ):
        """
        create or get table in vectordb
        """
        try:
            table = db.describe_table(table_name)
            return table
        except ServerError as e:
            if e.code == ServerErrCode.TABLE_NOT_EXIST:
                pass
            else:
                raise
        table = db.create_table(table_name, replication, partition, schema)

        while True:
            time.sleep(1)
            table = db.describe_table(table_name)
            if table.state == TableState.NORMAL:
                break

        return table

    def create_or_get_meta_table(self, db_name, table_name):
        """
        Ensures the document metadata table is created or exists
        """
        db = self.create_or_get_db(db_name)
        fields = []
        fields.append(
            Field(
                "kb_id",
                FieldType.UUID,
                primary_key=True,
                partition_key=True,
                auto_increment=False,
                not_null=True,
            )
        )
        fields.append(
            Field(
                "doc_id",
                FieldType.UUID,
                primary_key=True,
                partition_key=False,
                auto_increment=False,
                not_null=True,
            )
        )
        fields.append(
            Field(
                "doc_name",
                FieldType.TEXT,
                not_null=True,
            )
        )
        fields.append(Field("doc_type", FieldType.STRING))
        fields.append(Field("layout", FieldType.STRING))
        fields.append(Field("lang", FieldType.STRING))
        fields.append(Field("file_path", FieldType.STRING))
        fields.append(Field("uri", FieldType.STRING))
        fields.append(Field("size", FieldType.UINT32))
        fields.append(Field("ctime", FieldType.UINT64))
        indexes = []
        indexes.append(SecondaryIndex(index_name="doc_name_idx", field="doc_name"))

        table = self.create_or_get_table(
            db=db,
            table_name=table_name,
            schema=Schema(fields=fields, indexes=indexes)
        )
        logger.debug(f"create table:{table_name}")
        return table
    
    def create_or_get_chunk_table(
        self,
        db_name,
        table_name,
        partition_num=1,
        replication=1,
        vector_dimension=384
    ):
        """
        Ensures the document chunk table is created or exists
        """
        db = self.create_or_get_db(db_name)
        fields = []
        fields.append(
            Field(
                "kb_id",
                FieldType.UUID,
                primary_key=True,
                partition_key=True,
                auto_increment=False,
                not_null=True,
            )
        )
        fields.append(
            Field(
                "doc_id",
                FieldType.UUID,
                primary_key=True,
                partition_key=False,
                auto_increment=False,
                not_null=True,
            )
        )
        fields.append(
            Field(
                "chunk_id",
                FieldType.STRING,
                primary_key=True,
                partition_key=False,
                auto_increment=False,
                not_null=True,
            )
        )
        fields.append(
            Field(
                "doc_name",
                FieldType.TEXT,
                not_null=True,
            )
        )
        fields.append(
            Field(
                "sequence_number",
                FieldType.UINT32,
                not_null=True,
            )
        )
        fields.append(Field("size", FieldType.UINT32))
        fields.append(
            Field(
                "content",
                FieldType.TEXT,
                not_null=True,
            )
        )
        fields.append(Field("content_len", FieldType.UINT32))
        fields.append(
            Field(
                "embedding",
                FieldType.FLOAT_VECTOR,
                dimension=vector_dimension,
                not_null=True,
            )
        )
        fields.append(Field("ctime", FieldType.UINT64))
        indexes = []
        indexes.append(
            VectorIndex(
                index_name="embedding_idx",
                field="embedding",
                index_type=IndexType.FLAT,
                metric_type=MetricType.IP,
                auto_build=False,
            )
        )
        indexes.append(SecondaryIndex(index_name="doc_name_idx", field="doc_name"))
        indexes.append(
            SecondaryIndex(
                index_name="sequence_number_idx",
                field="sequence_number"
            )
        )
        indexes.append(
            InvertedIndex(
                index_name="content_inverted_idx",
                fields=["content"],
                params=InvertedIndexParams(
                    analyzer=InvertedIndexAnalyzer.DEFAULT_ANALYZER,
                    parse_mode=InvertedIndexParseMode.COARSE_MODE
                ),
                field_attributes=[InvertedIndexFieldAttribute.ANALYZED]
            )
        )

        table = self.create_or_get_table(
            db=db,
            table_name=table_name,
            replication=replication,
            partition=Partition(partition_num=partition_num),
            schema=Schema(fields=fields, indexes=indexes)
        )
        return table

    def ingest_doc_by_qianfan(self):
        """ingest doc by qianfan"""
        env = DocumentHubEnv(root_path="local://your_root_path/")
        doc_hub = LocalDocumentHub(env=env)
        kb_id = str(uuid.uuid4())
        doc = Document(
            kb_id=kb_id, # 知识库ID
            doc_name="test/test.pdf", # 文档名，指期望文档存储在文档管理中的相对路径，举例：test/test.pdf
            file_path="./test.pdf" # 文档的本地路径
        )
        doc = doc_hub.add(doc=doc)
        
        os.environ["APPBUILDER_TOKEN"] = "your_ab_token"
        doc_processor = QianfanDocProcessor(maximum_page_length=300, page_overlap_length=50)
        embedder = QianfanEmbedder(batch=2)
        pipeline = DefaultPipeline()
        
        db_name = "DocumentInsight"
        meta_table_name = "KnowledgeBase_Meta"
        chunk_table_name = "KnowledgeBase_Chunk"
        meta_table = self.create_or_get_meta_table(db_name=db_name, table_name=meta_table_name)
        chunk_table = self.create_or_get_chunk_table(db_name=db_name, table_name=chunk_table_name)
        pipeline.ingest_doc(
            doc=doc, 
            doc_processor=doc_processor, 
            embedder=embedder,
            meta_table=meta_table,
            chunk_table=chunk_table
        )
        doc_hub.remove(doc=doc)
    
    def ingest_doc_by_langchain(self):
        """ingest doc by langchain"""
        env = DocumentHubEnv(root_path="local://your_root_path")
        doc_hub = LocalDocumentHub(env=env)
        kb_id = str(uuid.uuid4())
        doc = Document(
            kb_id=kb_id, # 知识库ID
            doc_name="test/test.pdf", # 文档名，指期望文档存储在文档管理中的相对路径，举例：test/test.pdf
            file_path="./data/RAG.pdf" # 文档的本地路径
        )
        doc = doc_hub.add(doc=doc)
        
        doc_processor = LangchainDocProcessor(maximum_page_length=300, page_overlap_length=50)
        os.environ["APPBUILDER_TOKEN"] = "your ab token"
        embedder = QianfanEmbedder(batch=2)
        pipeline = DefaultPipeline()
        
        db_name = "DocumentInsight"
        meta_table_name = "KnowledgeBase_Meta"
        chunk_table_name = "KnowledgeBase_Chunk"
        meta_table = self.create_or_get_meta_table(db_name=db_name, table_name=meta_table_name)
        chunk_table = self.create_or_get_chunk_table(db_name=db_name, table_name=chunk_table_name)
        pipeline.ingest_doc(
            doc=doc, 
            doc_processor=doc_processor, 
            embedder=embedder,
            meta_table=meta_table,
            chunk_table=chunk_table
        )
        doc_hub.remove(doc=doc)
    
    def vector_search(self):
        """vector search"""
        os.environ["APPBUILDER_TOKEN"] = "your ab token"
        embedder = QianfanEmbedder(batch=2)
        pipeline = DefaultPipeline()
        
        db_name = "DocumentInsight"
        chunk_table_name = "KnowledgeBase_Chunk"
        chunk_table = self.create_or_get_chunk_table(db_name=db_name, table_name=chunk_table_name)
        
        search_contents = ["your question"]
        search_request = VectorTopkSearchRequest(
            vector_field="embedding",
            limit=10, 
            #filter="kb_id='xxx'",  填入标量过滤条件
            config=VectorSearchConfig(ef=200)
        )
        result = pipeline.vector_search(
            search_contents=search_contents, 
            embedder=embedder,
            table=chunk_table,
            search_request=search_request
        )
        logger.debug("vector search result: {}".format(result))

    def bm25_search(self):
        """bm25 search"""
        pipeline = DefaultPipeline()
        
        db_name = "DocumentInsight"
        chunk_table_name = "KnowledgeBase_Chunk"
        chunk_table = self.create_or_get_chunk_table(db_name=db_name, table_name=chunk_table_name)
        search_request = BM25SearchRequest(
            index_name="content_inverted_idx",
            search_text="bm25 search text",
            limit=10,
            #filter="kb_id='xxx'"
        )
        result = pipeline.bm25_search(
            table=chunk_table,
            search_request=search_request
        )
        logger.debug("bm25 search result: {}".format(result))

    def hybrid_search(self):
        """hybrid search"""
        os.environ["APPBUILDER_TOKEN"] = "your_ab_token"
        embedder = QianfanEmbedder(batch=2)
        pipeline = DefaultPipeline()
        
        db_name = "DocumentInsight"
        chunk_table_name = "KnowledgeBase_Chunk"
        chunk_table = self.create_or_get_chunk_table(db_name=db_name, table_name=chunk_table_name)
        search_contents = ["your question"]
        vector_search_request = VectorTopkSearchRequest(
            vector_field="embedding",
            config=VectorSearchConfig(ef=200)
        )
        bm25_search_request = BM25SearchRequest(
            index_name="content_inverted_idx",
            search_text="bm25 search text"
        )
        search_request = HybridSearchRequest(
            vector_request=vector_search_request,
            vector_weight=0.4, # 向量检索比重
            bm25_request=bm25_search_request,
            bm25_weight=0.6,
            #filter="kd_id='xxx'", 填入标量过滤条件
            limit=15
        )
        result = pipeline.hybrid_search(
            search_contents=search_contents,
            embedder=embedder,
            table=chunk_table,
            search_request=search_request
        )
        logger.debug("hybrid_ search result: {}".format(result))

    def drop_db(self):
        """
        drop database and table in vectordb
        """
        db = None
        try:
            db = self._client.database('DocumentInsight')
        except ClientError as e:
            logger.debug("database {} not found.".format('book'))
            pass
        
        if db is not None:
            try:
                db.drop_table('KnowledgeBase_Meta')
            except ServerError as e:
                logger.debug("drop table error {}".format(e))
                if e.code == ServerErrCode.TABLE_NOT_EXIST:
                    pass
            try:
                db.drop_table('KnowledgeBase_Chunk')
            except ServerError as e:
                logger.debug("drop table error {}".format(e))
                if e.code == ServerErrCode.TABLE_NOT_EXIST:
                    pass
            time.sleep(10)
            db.drop_database()

if __name__ == "__main__":
    account = 'root'
    api_key = 'mcqlwzjtxsrbsj'
    endpoint = 'http://10.147.81.230:8287' #example:http://127.0.0.1:8511

    config = Configuration(credentials=BceCredentials(account, api_key),
            endpoint=endpoint)
    demo = AiSearchDemo(config)
    
    #demo.drop_db()
    #demo.manage_document_in_local()
    #demo.manage_document_in_bos()
    #demo.parser_document()
    #demo.embedding_document()
    #demo.ingest_doc_by_qianfan()
    #demo.ingest_doc_by_langchain()
    demo.vector_search()
    demo.bm25_search()
    demo.hybrid_search()
    #demo.drop_db()
