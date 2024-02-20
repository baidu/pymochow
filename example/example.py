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
from pymochow.model.schema import Schema, Field, SecondaryIndex, VectorIndex, HNSWParams
from pymochow.model.enum import FieldType, IndexType, MetricType, ServerErrCode
from pymochow.model.enum import TableState, IndexState
from pymochow.model.table import Partition, Row, AnnSearch, HNSWSearchParams

logging.basicConfig(filename='example.log', level=logging.DEBUG,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TestMochow:
    def __init__(self, config):
        """
        init mochow client
        """
        self._client = pymochow.MochowClient(config)
    
    def clear(self):
        db = None
        try:
            db = self._client.database('book')
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
            db.drop_database()

    def create_db_and_table(self):
        """create database&table"""
        database = 'book'
        table_name = 'book_segments'

        db = self._client.create_database(database)

        database_list = self._client.list_databases()
        for db_item in database_list:
            logger.debug("database: {}".format(db_item.database_name))
        
        fields = []
        fields.append(Field("id", FieldType.STRING, primary_key=True,
            partition_key=True, auto_increment=False, not_null=True))
        fields.append(Field("bookName", FieldType.STRING, not_null=True))
        fields.append(Field("author", FieldType.STRING))
        fields.append(Field("page", FieldType.UINT32))
        fields.append(Field("segment", FieldType.STRING))
        fields.append(Field("vector", FieldType.FLOAT_VECTOR, dimension=3))
        indexes = []
        indexes.append(VectorIndex(index_name="vector_idx", index_type=IndexType.HNSW,
            field="vector", metric_type=MetricType.L2, 
            params=HNSWParams(m=32, efconstruction=200)))
        indexes.append(SecondaryIndex(index_name="book_name_idx", field="bookName"))

        db.create_table(
            table_name=table_name,
            replication=3,
            partition=Partition(partition_num=3),
            schema=Schema(fields=fields, indexes=indexes)
        )

        while True:
            time.sleep(2)
            table = db.describe_table(table_name)
            if table.state == TableState.NORMAL:
                break
        
        time.sleep(10)
        logger.debug("table: {}".format(table.to_dict()))

    def upsert_data(self):
        """upsert data"""
        db = self._client.database('book')
        table = db.table('book_segments')

        rows = [
            Row(id='0001',
                vector=[0.2123, 0.21, 0.213],
                bookName='西游记',
                author='吴承恩',
                page=21,
                segment='富贵功名，前缘分定，为人切莫欺心。'),
            Row(id='0002',
                vector=[0.2123, 0.22, 0.213],
                bookName='西游记',
                author='吴承恩',
                page=22,
                segment='正大光明，忠良善果弥深。些些狂妄天加谴，眼前不遇待时临。'),
            Row(id='0003',
                vector=[0.2123, 0.23, 0.213],
                bookName='三国演义',
                author='罗贯中',
                page=23,
                segment='细作探知这个消息，飞报吕布。'),
            Row(id='0004',
                vector=[0.2123, 0.24, 0.213],
                bookName='三国演义',
                author='罗贯中',
                page=24,
                segment='布大惊，与陈宫商议。宫曰：“闻刘玄德新领徐州，可往投之。”' \
                        '布从其言，竟投徐州来。有人报知玄德。'),
            Row(id='0005',
                vector=[0.2123, 0.25, 0.213],
                bookName='三国演义',
                author='罗贯中',
                page=25,
                segment='玄德曰：“布乃当今英勇之士，可出迎之。”' \
                '糜竺曰：“吕布乃虎狼之徒，不可收留；收则伤人矣。'),
        ]
        table.upsert(rows=rows)
        time.sleep(1)

    def query_data(self):
        """query data"""
        db = self._client.database('book')
        table = db.table('book_segments')

        primary_key = {'id':'0001'}
        projections = ["id", "bookName"]
        res = table.query(primary_key=primary_key, projections=projections, 
                retrieve_vector=True)
        logger.debug("res: {}".format(res))

    def search_data(self):
        """search data"""
        db = self._client.database('book')
        table = db.table('book_segments')
        
        table.rebuild_index("vector_idx")
        while True:
            time.sleep(2)
            index = table.describe_index("vector_idx")
            if index.state == IndexState.NORMAL:
                break

        anns = AnnSearch(vector_field="vector", vector_floats=[0.3123, 0.43, 0.213],
            params=HNSWSearchParams(ef=200, limit=10), filter="bookName='三国演义'")
        res = table.search(anns=anns)
        logger.debug("res: {}".format(res))

    def delete_data(self):
        """delete data"""
        db = self._client.database('book')
        table = db.table('book_segments')

        primary_key = {'id': '0001'}
        res = table.delete(primary_key=primary_key)
        logger.debug("res: {}".format(res))
    
    def drop_and_create_vindex(self):
        """drop and create vindex"""
        db = self._client.database('book')
        table = db.table('book_segments')
        table.drop_index("vector_idx")
        while True:
            time.sleep(2)
            try:
                index = table.describe_index("vector_idx")
            except ServerError as e:
                logger.debug("code: {}".format(e.code))
                if e.code == ServerErrCode.INDEX_NOT_EXIST:
                    break
        
        indexes = []
        indexes.append(VectorIndex(index_name="vector_idx", index_type=IndexType.HNSW,
            field="vector", metric_type=MetricType.L2, 
            params=HNSWParams(m=16, efconstruction=200), auto_build=False))
        table.create_indexes(indexes)
        time.sleep(1)
        table.modify_index(index_name="vector_idx", auto_build=True)
        index = table.describe_index("vector_idx")
        logger.debug("index: {}".format(index.to_dict()))

    def delete_and_drop(self):
        """delete and drop"""
        db = self._client.database('book')

        db.drop_table('book_segments')
        time.sleep(10)

        db.drop_database()
        self._client.close()

if __name__ == "__main__":
    account = 'root'
    api_key = 'your api key'
    endpoint = 'your endpoint' #example:http://127.0.0.1:8511

    config = Configuration(credentials=BceCredentials(account, api_key),
            endpoint=endpoint)
    test_vdb = TestMochow(config)
    test_vdb.clear()
    test_vdb.create_db_and_table()
    test_vdb.upsert_data()
    test_vdb.query_data()
    test_vdb.search_data()
    test_vdb.delete_data()
    test_vdb.drop_and_create_vindex()
    test_vdb.delete_and_drop()

