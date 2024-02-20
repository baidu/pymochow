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
This module provide table model.
"""
import copy
import orjson
from pymochow import utils
from pymochow import client
from pymochow.http import http_methods
from pymochow.model.schema import VectorIndex, SecondaryIndex, HNSWParams
from pymochow.model.enum import PartitionType, ReadConsistency
from pymochow.model.enum import IndexType, IndexState, MetricType


class Partition:
    """
    Partition
    """
    def __init__(self, partition_num, partition_type=PartitionType.HASH):
        self._partition_num = partition_num
        self._partition_type = partition_type
    
    def to_dict(self):
        """to dict"""
        res = {
            "partitionType": self._partition_type,
            "partitionNum": self._partition_num
        }
        return res


class Table:
    """
    Table
    """
    def __init__(
            self,
            db,
            name,
            replication,
            partition,
            schema,
            enable_dynamic_field=False,
            description='',
            config=None,
            **kwargs):
        self._conn = db.conn
        self._database_name = db.database_name
        self._table_name = name
        self._replication = replication
        self._partition = partition
        self._schema = schema
        self._enable_dynamic_field = enable_dynamic_field
        self._description = description
        self._config = config
        self._create_time = kwargs.get('create_time', '')
        self._state = kwargs.get('state', None)
        self._aliases = kwargs.get('aliases', [])

    @property
    def conn(self):
        """http conn"""
        return self._conn
    
    @property
    def database_name(self):
        """database name"""
        return self._database_name

    @property
    def table_name(self):
        """table name"""
        return self._table_name

    @property
    def schema(self):
        """schema"""
        return self._schema
    
    @property
    def replication(self):
        """replication"""
        return self._replication
    
    @property
    def partition(self):
        """partition"""
        return self._partition
    
    @property
    def enable_dynamic_field(self):
        """enable dynamic field"""
        return self._enable_dynamic_field
    
    @property
    def description(self):
        """description"""
        return self._description
    
    @property
    def create_time(self):
        """create time"""
        return self._create_time
    
    @property
    def state(self):
        """state"""
        return self._state

    @property
    def aliases(self):
        """aliases"""
        return self._aliases
    
    def to_dict(self):
        """to dict"""
        res = {
            "database": self.database_name,
            "table": self.table_name,
            "description": self.description,
            "replication": self.replication,
            "partition": self.partition.to_dict(),
            "enableDynamicField": self.enable_dynamic_field,
            "schema": self.schema.to_dict(),
            "aliases": self.aliases
        }
        if self.create_time != '':
            res["createTime"] = self.create_time
        if self.state is not None:
            res["state"] = self.state
        return res
    
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

    def insert(self, rows, config=None):
        """
        insert rows
        """
        if not self.conn:
            raise ClientError('conn is closed')

        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        body["rows"] = []

        for row in rows:
            body['rows'].append(row.to_dict())
        json_body = orjson.dumps(body)
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'row')

        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={b'insert': b''},
                config=config)
    
    def upsert(self, rows, config=None):
        """
        upsert rows
        """
        if not self.conn:
            raise ClientError('conn is closed')

        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        body["rows"] = []

        for row in rows:
            body['rows'].append(row.to_dict())
        json_body = orjson.dumps(body)
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'row')

        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={b'upsert': b''},
                config=config)
    
    def query(self, primary_key, partition_key=None, projections=None, 
            retrieve_vector=False, read_consistency=ReadConsistency.EVENTUAL, 
            config=None):
        """
        query
        """
        if not self.conn:
            raise ClientError('conn is closed')
        
        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        body["primaryKey"] = primary_key
        if partition_key is not None:
            body["partitionKey"] = partition_key
        if projections is not None:
            body["projections"] = projections
        body["retrieveVector"] = retrieve_vector
        body["readConsistency"] = read_consistency
        json_body = orjson.dumps(body)
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'row')

        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={b'query': b''},
                config=config)
    
    def search(self, anns, partition_key=None, projections=None, 
            retrieve_vector=False, read_consistency=ReadConsistency.EVENTUAL, 
            config=None):
        """
        search
        """
        if not self.conn:
            raise ClientError('conn is closed')

        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        body["anns"] = anns.to_dict()
        if partition_key is not None:
            body["partitionKey"] = partition_key
        if projections is not None:
            body["projections"] = projections
        body["retrieveVector"] = retrieve_vector
        body["readConsistency"] = read_consistency
        json_body = orjson.dumps(body)
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'row')
        
        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={b'search': b''},
                config=config)
    
    def delete(self, primary_key, partition_key=None, config=None):
        """
        delete row
        """
        if not self.conn:
            raise ClientError('conn is closed')
        
        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        body["primaryKey"] = primary_key
        if partition_key is not None:
            body["partitionKey"] = partition_key
        json_body = orjson.dumps(body)
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'row')

        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={b'delete': b''},
                config=config)
    
    def create_indexes(self, indexes, config=None):
        """
        create indexes
        """
        if not self.conn:
            raise ClientError('conn is closed')
        
        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        body["indexes"] = []

        for index in indexes:
            if isinstance(index, VectorIndex):
                body["indexes"].append(index.to_dict())
            else:
                raise ClientError("not supported index type")
        
        json_body = orjson.dumps(body)
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'index')
        
        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={b'create': b''},
                config=config)
    
    def modify_index(self, index_name, auto_build, config=None):
        """
        modify index
        """
        if not self.conn:
            raise ClientError('conn is closed')
        
        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        body["index"] = {
            "indexName": index_name,
            "autoBuild": auto_build
        }
        json_body = orjson.dumps(body)
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'index')
        
        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={b'modify': b''},
                config=config)

    def drop_index(self, index_name, config=None):
        """drop index"""
        if not self.conn:
            raise ClientError('conn is closed')

        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'index')
        return self.conn.send_request(http_methods.DELETE,
                path=uri,
                params={
                    b'database': self.database_name,
                    b'table': self.table_name,
                    b'indexName': index_name},
                config=config)

    def rebuild_index(self, index_name, config=None):
        """build vector index"""
        if not self.conn:
            raise ClientError('conn is closed')
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'index')
        
        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=orjson.dumps({"database": self.database_name,
                    "table": self.table_name,
                    "indexName": index_name}),
                params={b'rebuild': b''},
                config=config)

    def describe_index(self, index_name, config=None):
        """describe index"""
        if not self.conn:
            raise ClientError('conn is closed')
        
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'index')
        
        response = self.conn.send_request(http_methods.POST,
                path=uri,
                params={b'desc': b''},
                body=orjson.dumps({
                    'database': self.database_name,
                    'table': self.table_name,
                    'indexName': index_name
                }),
                config=config)
        index = response.index
        if index["indexType"] == IndexType.HNSW.value:
            return VectorIndex(
                index_name=index["indexName"],
                index_type=IndexType.HNSW,
                field=index["field"],
                metric_type=getattr(MetricType, index["metricType"], None),
                params=HNSWParams(m=index["params"]["M"], 
                    efconstruction=index["params"]["efConstruction"]),
                auto_build=index["autoBuild"],
                state=getattr(IndexState, index["state"], None))
        elif index["indexType"] == IndexType.FLAT.value:
            return VectorIndex(
                index_name=index["indexName"],
                index_type=IndexType.FLAT,
                field=index["field"],
                metric_type=getattr(MetricType, index["metricType"], None),
                auto_build=index["autoBuild"],
                state=getattr(IndexState, index["state"], None))
        elif index["indexType"] == IndexType.SECONDARY_INDEX.value:
            return SecondaryIndex(
                index_name=index["indexName"],
                field=index["field"])
        else:
            raise ClientError("not supported index type:%s" % (index["indexType"]))


class Row:
    """
    row, the object for document insert, query and search, the parameter depends on
    the schema of table.
    """

    def __init__(self, **kwargs) -> None:
        self._data = kwargs

    def to_dict(self):
        """to dict"""
        return self._data


class AnnSearch:
    """ann search"""

    def __init__(self, vector_field, vector_floats, params, filter=''):
        self._vector_field = vector_field
        self._vector_floats = vector_floats
        self._params = params
        self._filter = filter

    def to_dict(self):
        """to dict"""
        res = {
            'vectorField': self._vector_field,
            'vectorFloats': self._vector_floats,
            'params': self._params.to_dict()
        }
        if self._filter != '':
            res['filter'] = self._filter
        return res


class HNSWSearchParams:
    "hnsw search params"

    def __init__(self, ef=0, distance_far=None, distance_near=None, limit=50, 
            pruning=True):
        self._ef = ef
        self._distance_far = distance_far
        self._distance_near = distance_near
        self._limit = limit
        self._pruning = pruning

    def to_dict(self):
        """to dict"""
        res = {}
        if self._ef > 0:
            res['ef'] = self._ef
        if self._distance_far is not None:
            res['distanceFar'] = self._distance_far
        if self._distance_near is not None:
            res['distanceNear'] = self._distance_near
        res['limit'] = self._limit
        res['pruning'] = self._pruning
        return res
