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
This module provide database model.
"""
import copy
import orjson
import logging
from typing import List
from pymochow.exception import ClientError, ServerError
from pymochow import utils
from pymochow import client
from pymochow.http import http_methods
from pymochow.model.table import Table, Partition
from pymochow.model.schema import Schema, Field, VectorIndex, SecondaryIndex, HNSWParams, PUCKParams
from pymochow.model.enum import IndexType, MetricType, TableState

_logger = logging.getLogger(__name__)


class Database:
    """Database Model"""

    def __init__(self, conn, database_name='', config=None):
        self._database_name = database_name
        self._conn = conn
        self._config = config

    @property
    def conn(self):
        """http connection"""
        return self._conn

    @property
    def database_name(self):
        """database name"""
        return self._database_name
    
    def _merge_config(self, config):
        """merge config
        Args:
            config (dict): config need merge
        Returns:
            dict：merged config
        """
        if config is None:
            return self._config
        else:
            new_config = copy.copy(self._config)
            new_config.merge_non_none_values(config)
            return new_config

    def create_database(self, config=None):
        """Create database.
        Args:
            database_name(str): database name
            config(Optional[Configuration]): client configuration
        """
        if not self.conn:
            raise ClientError('conn is closed')

        if not self.database_name:
            raise ClientError('database name param not found')
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'database')

        self.conn.send_request(http_methods.POST,
                path=uri,
                body=orjson.dumps({'database': self.database_name}),
                params={b'create': b''},
                config=config)
    
    def drop_database(self, config=None):
        """Drop database.
        Args:
            database_name(str): database name
            config(Optional[Configuration]): client configuration
        """
        if not self.conn:
            raise ClientError('conn is closed')

        if not self.database_name:
            raise ClientError('database name param not found')
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'database')
        self.conn.send_request(http_methods.DELETE,
                path=uri,
                params={b'database': self.database_name},
                config=config)
    
    def list_databases(self, config=None) -> List:
        """List databases.
        Args:
            config(Optional[Configuration]): client configuration
        Return:
            List: database list
        """
        if not self.conn:
            raise ClientError('conn is closed')

        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'database')
        response = self.conn.send_request(http_methods.POST,
                path=uri,
                params={b'list': b''},
                config=config)
        res = []
        for database in response.databases:
            res.append(Database(self.conn, database_name=database, config=self._config))
        return res

    def create_table(self, table_name, replication, partition, schema,
            enable_dynamic_field=False, description=None, config=None) -> Table:
        """create table
        Args:
            table_name(str): table name
            replication(int): replica number
            partition(Partition): partiton strategy
            schema(Schema): table schema
            enable_dynamic_field(boolean): enable dynamic add field
            description(Optional[str]): table description
            config(Optional[Configuration]): client configuration
        Return:
            Table: table
        """
        if not self.conn:
            raise ClientError('conn is closed')

        if not self.database_name:
            raise ClientError('database name param not found')
        
        if not table_name:
            raise ClientError('table name param not found')

        if not schema:
            raise ClientError('table schema param not found')

        body = {}
        body["database"] = self.database_name
        body["table"] = table_name
        body["replication"] = replication
        body["partition"] = partition.to_dict()
        body["schema"] = schema.to_dict()
        body["enableDynamicField"] = enable_dynamic_field

        if description is not None:
            body["description"] = description

        json_body = orjson.dumps(body)

        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'table')
        
        try:
            response = self.conn.send_request(http_methods.POST,
                    path=uri,
                    params={b'create': b''},
                    body=json_body,
                    config=config)
            _logger.debug(b"response:%s", response)
        except ServerError as e:
            _logger.debug("create table error:%s", e)
            raise e

        return Table(self, table_name, replication, partition, schema, 
                enable_dynamic_field=enable_dynamic_field,
                description=description,
                config=self._config)
    
    def drop_table(self, table_name, config=None):
        """drop table
        Args:
            table_name(str): table name
            config(Optional[Configuration]): client configuration
        """
        if not self.conn:
            raise ClientError('conn is closed')

        if not self.database_name:
            raise ClientError('database name param not found')
        
        if not table_name:
            raise ClientError('table name param not found')
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'table')

        return self.conn.send_request(http_methods.DELETE,
                path=uri,
                params={
                    b'database': self.database_name,
                    b'table': table_name},
                config=config)

    def describe_table(self, table_name, config=None) -> Table:
        """describe table
        Args:
            table_name(str): table name
            config(Optional[Configuration]): client configuration
        Return:
            Table: table
        """
        if not self.conn:
            raise ClientError('conn is closed')

        if not self.database_name:
            raise ClientError('database name param not found')
        
        if not table_name:
            raise ClientError('table name param not found')
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'table')
        
        response = self.conn.send_request(http_methods.POST,
                path=uri,
                params={b'desc': b''},
                body=orjson.dumps({'database': self.database_name,
                    'table': table_name}),
                config=config)
        table = response.table
        partition = Partition(partition_num=table["partition"]["partitionNum"])
        
        fields = []
        for field in table["schema"]["fields"]:
            fields.append(Field(
                field_name=field["fieldName"], 
                field_type=field["fieldType"],
                primary_key=(
                    field["primaryKey"]
                    if "primaryKey" in field else False
                ), 
                partition_key=(
                    field["partitionKey"]
                    if "partitionKey" in field else False
                ), 
                auto_increment=(
                    field["autoIncrement"]
                    if "autoIncrement" in field else False
                ),
                not_null=(
                    field["notNull"]
                    if "notNull" in field else False
                ), 
                dimension=(
                    field["dimension"]
                    if "dimension" in field else 0
                )))
        
        indexes = []
        for index in table["schema"]["indexes"]:
            if index["indexType"] == IndexType.HNSW.value:
                indexes.append(VectorIndex(
                    index_name=index["indexName"],
                    index_type=IndexType.HNSW,
                    field=index["field"],
                    metric_type=getattr(MetricType, index["metricType"], None),
                    params=HNSWParams(m=index["params"]["M"], 
                        efconstruction=index["params"]["efConstruction"]),
                    auto_build=index["autoBuild"]))
            elif index["indexType"] == IndexType.FLAT.value:
                indexes.append(VectorIndex(
                    index_name=index["indexName"],
                    index_type=IndexType.FLAT,
                    field=index["field"],
                    metric_type=getattr(MetricType, index["metricType"], None),
                    auto_build=index["autoBuild"]))
            elif index["indexType"] == IndexType.PUCK.value:
                indexes.append(VectorIndex(
                    index_name=index["indexName"],
                    index_type=IndexType.PUCK,
                    field=index["field"],
                    metric_type=getattr(MetricType, index["metricType"], None),
                    params=PUCKParams(coarseClusterCount=index["params"]["coarseClusterCount"], 
                        fineClusterCount=index["params"]["fineClusterCount"]),
                    auto_build=index["autoBuild"]))
            elif index["indexType"] == IndexType.SECONDARY_INDEX.value:
                indexes.append(SecondaryIndex(
                    index_name=index["indexName"],
                    field=index["field"]))
            else:
                raise ClientError("not supported index type:%s" % (index["indexType"]))

        schema = Schema(fields=fields, indexes=indexes)
        return Table(self, table_name, table["replication"], partition, schema, 
                enable_dynamic_field=(
                    table["enableDynamicField"] 
                    if "enableDynamicField" in table else False
                ),
                description=table["description"],
                config=self._config,
                create_time=table["createTime"],
                state=getattr(TableState, table["state"], None),
                aliases=table["aliases"])
    
    def table(self, table_name, config=None) -> Table:
        """get table
        Args:
            table_name(str): table name
            config(Optional[Configuration]): client configuration
        Return:
            Table: table
        """
        return self.describe_table(table_name, config)

    def list_table(self, config=None) -> List:
        """list table
        """
        if not self.conn:
            raise ClientError('conn is closed')
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'table')
        
        response = self.conn.send_request(http_methods.POST,
                path=uri,
                params={b'list': b''},
                body=orjson.dumps({'database': self.database_name}),
                config=config)
        
        res = []
        for table_name in response.tables:
            res.append(self.table(table_name, config))
        return res

