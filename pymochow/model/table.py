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
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union, Tuple
import copy
import orjson
from pymochow import utils
from pymochow import client
from pymochow.http import http_methods
from pymochow.model.schema import (
    VectorIndex,
    SecondaryIndex,
    HNSWParams,
    HNSWPQParams,
    PUCKParams,
    DefaultAutoBuildPolicy,
    AutoBuildTool,
    InvertedIndex,
    InvertedIndexParams,
    InvertedIndexAnalyzer,
    InvertedIndexParseMode
)
from pymochow.model.enum import (PartitionType, ReadConsistency,
    IndexType, IndexState, MetricType, AutoBuildPolicyType, RequestType)
from pymochow.exception import ClientError

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


@utils.deprecated("No longer used. Use SearchRequest instead.")
class AnnSearch:
    """ann search"""

    def __init__(self, vector_field, vector_floats, params, filter=None):
        """init"""
        self._vector_field = vector_field
        self._vector_floats = vector_floats
        self._params = params
        self._filter = filter

    def to_dict(self):
        """to dict"""
        res = {
            'vectorField': self._vector_field,
            'vectorFloats': self._vector_floats,
            'params': self._params.to_dict(),
        }
        if self._filter is not None:
            res['filter'] = self._filter
        return res


class Vector(ABC):
    """base class of vector"""
    @abstractmethod
    def representation(self):
        """representation"""
        pass


class BatchQueryKey:
    '''BatchQueryKey'''

    def __init__(self, primary_key, partition_key=None):
        '''init'''
        self._primary_key = primary_key
        self._partition_key = partition_key

    def to_dict(self):
        '''to dict'''

        res = {'primaryKey': self._primary_key}
        if self._partition_key is not None:
            res['partitionKey'] = self._partition_key
        return res


class FloatVector(Vector):
    """float vector"""

    def __init__(self, floats: List[float]):
        """init"""
        self._floats = floats

    def representation(self):
        """representation"""
        return self._floats


class VectorSearchConfig:
    """
    Optional configurable params for vector search.

    For each index algorithm, the params that could be set are:

    | IndexType | Params              |
    |-----------+---------------------|
    | HNSW      | ef, pruning         |
    | HNSWPQ    | ef, pruning         |
    | PUCK      | search_coarse_count |
    | FLAT      |                     |

    """

    def __init__(self, *,
                 ef: int = None,
                 pruning: bool = None,
                 search_coarse_count: int = None):
        """init"""
        self._ef = ef
        self._pruning = pruning
        self._search_coarse_count = search_coarse_count

    def to_dict(self):
        """to_dict"""
        res = {}
        if self._ef is not None:
            res['ef'] = self._ef
        if self._pruning is not None:
            res['pruning'] = self._pruning
        if self._search_coarse_count is not None:
            res['searchCoarseCount'] = self._search_coarse_count
        return res


class SearchRequest(ABC):
    """base class"""
    @abstractmethod
    def to_dict(self) -> Dict:
        """to_dict"""
        pass

    @abstractmethod
    def type(self) -> RequestType:
        """type"""
        pass


class VectorTopkSearchRequest(SearchRequest):
    """ TopK search request """

    def __init__(self, *,
                 vector_field: str,
                 vector: Vector = None,
                 limit: int = 50,
                 filter: str = None,
                 config: VectorSearchConfig = None):
        """init"""
        self._vector_field = vector_field
        self._vector = vector
        self._limit = limit
        self._filter = filter
        self._config = config
    
    @property
    def vector(self):
        """get vector"""
        return self._vector

    @vector.setter
    def vector(self, vector: Vector):
        """set vector"""
        self._vector = vector

    def to_dict(self):
        """to_dict"""
        res = dict()

        anns = {
            "vectorField": self._vector_field,
            "vectorFloats": self._vector.representation(),
        }

        if self._filter is not None:
            anns["filter"] = self._filter

        params = dict()
        if self._config is not None:
            params = self._config.to_dict()
        if self._limit is not None:
            params["limit"] = self._limit

        if len(params) != 0:
            anns["params"] = params

        res["anns"] = anns
        return res

    def type(self):
        return RequestType.SEARCH


class VectorRangeSearchRequest(SearchRequest):
    """ range search request """

    def __init__(self, *,
                 vector_field: str,
                 distance_range: Tuple[float, float],
                 vector: Vector = None,
                 limit: int = None,
                 filter: str = None,
                 config: VectorSearchConfig = None):
        """init"""
        self._vector_field = vector_field
        self._vector = vector
        self._distance_near = distance_range[0]
        self._distance_far = distance_range[1]
        self._limit = limit
        self._filter = filter
        self._config = config
    
    @property
    def vector(self):
        """get vector"""
        return self._vector

    @vector.setter
    def vector(self, vector: Vector):
        """set vector"""
        self._vector = vector

    def to_dict(self):
        """to_dict"""
        res = dict()

        anns = {
            "vectorField": self._vector_field,
            "vectorFloats": self._vector.representation(),
        }

        if self._filter is not None:
            anns["filter"] = self._filter

        params = dict()
        if self._config is not None:
            params = self._config.to_dict()
        if self._distance_near is not None:
            params["distanceNear"] = self._distance_near
        if self._distance_far is not None:
            params["distanceFar"] = self._distance_far
        if self._limit is not None:
            params["limit"] = self._limit

        if len(params) != 0:
            anns["params"] = params
        res["anns"] = anns

        return res

    def type(self):
        """type"""
        return RequestType.SEARCH


class VectorBatchSearchRequest(SearchRequest):
    """batch search request"""

    def __init__(self, *,
                 vector_field: str,
                 vectors: List[Vector] = None,
                 limit: int = None,
                 distance_range: Tuple[float, float] = None,
                 filter: str = None,
                 config: VectorSearchConfig = None):
        """init"""
        self._vector_field = vector_field
        self._vectors = vectors
        self._config = config
        self._limit = limit
        self._distance_range = distance_range
        self._filter = filter
    
    @property
    def vectors(self):
        """get vectors"""
        return self._vectors

    @vectors.setter
    def vectors(self, vectors: List[Vector]):
        """set vectors"""
        self._vectors = vectors

    def to_dict(self):
        """to_dict"""
        res = dict()

        vectors = []
        for vector in self._vectors:
            vectors.append(vector.representation())
        anns = {
            "vectorField": self._vector_field,
            "vectorFloats": vectors
        }
        if self._filter is not None:
            anns["filter"] = self._filter

        params = {}
        if self._config is not None:
            params = self._config.to_dict()
        if self._distance_range is not None:
            if self._distance_range[0] is not None:
                params["distanceNear"] = self._distance_range[0]
            if self._distance_range[1] is not None:
                params["distanceFar"] = self._distance_range[1]
        if self._limit is not None:
            params["limit"] = self._limit

        if len(params) != 0:
            anns["params"] = params

        res["anns"] = anns
        return res

    def type(self):
        """type"""
        return RequestType.BATCH_SEARCH


class BM25SearchRequest(SearchRequest):
    """BM25 search request"""

    def __init__(self, *,
                 index_name: str,
                 search_text: str,
                 limit: int = None,
                 filter: str = None):
        """init"""
        self._index_name = index_name
        self._search_text = search_text
        self._limit = limit
        self._filter = filter

    def to_dict(self):
        """to_dict"""
        res = {
            "BM25SearchParams": {
                "indexName": self._index_name,
                "searchText": self._search_text
            }
        }

        if self._limit is not None:
            res["limit"] = self._limit

        if self._filter is not None:
            res["filter"] = self._filter

        return res

    def type(self):
        """type"""
        return RequestType.SEARCH


VectorSearchRequest = Union[VectorTopkSearchRequest,
                            VectorRangeSearchRequest,
                            VectorBatchSearchRequest]


class HybridSearchRequest(SearchRequest):
    """Hybrid BM25 and vector search."""

    def __init__(self, *,
                 vector_request: VectorSearchRequest,
                 bm25_request: BM25SearchRequest,
                 vector_weight: float = 0.5,
                 bm25_weight: float = 0.5,
                 limit: int = None,
                 filter: str = None):
        """
        init

        'limit' and 'filter' are global settings, and they will
        apply to both vector search and BM25 search. Avoid setting
        them in 'bm25_request' or 'vector_request'.  Any settings in
        'vector_request' or 'bm25_request' for 'limit' or 'filter'
        will be overridden by the general settings.

        """

        self._vector_request = vector_request
        self._bm25_request = bm25_request
        self._vector_weight = vector_weight
        self._bm25_weight = bm25_weight
        self._limit = limit
        self._filter = filter

    def to_dict(self):
        """to_dict"""
        vector_search_params = self._vector_request.to_dict()
        vector_search_params["anns"]["weight"] = self._vector_weight

        bm25_search_params = self._bm25_request.to_dict()
        bm25_search_params["BM25SearchParams"]["weight"] = self._bm25_weight

        res = dict()
        res.update(vector_search_params)
        res.update(bm25_search_params)
        if self._limit is not None:
            res["limit"] = self._limit
        if self._filter is not None:
            res["filter"] = self._filter

        return res

    def type(self):
        """type"""
        return RequestType.SEARCH


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
                params={bytes(RequestType.INSERT): b''},
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
                params={'upsert': b''},
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
                params={bytes(RequestType.QUERY): b''},
                config=config)

    def batch_query(self, keys, projections=None,
                    retrieve_vector=False,
                    read_consistency=ReadConsistency.EVENTUAL,
                    config=None):
        """
        batch_query
        """
        if not self.conn:
            raise ClientError('conn is closed')

        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        body["keys"] = []
        for key in keys:
            body["keys"].append(key.to_dict())

        if projections is not None:
            body["projections"] = projections
        body["retrieveVector"] = retrieve_vector
        body["readConsistency"] = read_consistency
        json_body = orjson.dumps(body)
        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'row')

        return self.conn.send_request(http_methods.POST, path=uri,
                                      body=json_body,
                                      params={b'batchQuery': b''},
                                      config=config)


    @utils.deprecated("Use 'vector_search' instead.")
    def search(self, anns, partition_key=None, projections=None,
            retrieve_vector=False, read_consistency=ReadConsistency.EVENTUAL,
            config=None):
        """
        Deprecated. Use 'vector_search' instead.
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
                params={bytes(RequestType.SEARCH): b''},
                config=config)

    def vector_search(self, *,
                      request: VectorSearchRequest,
                      partition_key: Dict[str, Any] = None,
                      projections: List[str] = None,
                      read_consistency: ReadConsistency = ReadConsistency.EVENTUAL,
                      config: Dict[Any, Any] = None):
        """vector search"""
        if not isinstance(request, VectorSearchRequest.__args__):
            raise ValueError("wrong type of argument 'request'")

        return self._search(request=request,
                            partition_key=partition_key,
                            projections=projections,
                            read_consistency=read_consistency,
                            config=config)

    def bm25_search(self, *,
                    request: BM25SearchRequest,
                    partition_key: Dict[str, Any] = None,
                    projections: List[str] = None,
                    read_consistency: ReadConsistency = ReadConsistency.EVENTUAL,
                    config: Dict[Any, Any] = None):
        """BM25 search"""
        if not isinstance(request, BM25SearchRequest):
            raise ValueError("wrong type of argument 'request'")

        return self._search(request=request,
                            partition_key=partition_key,
                            projections=projections,
                            read_consistency=read_consistency,
                            config=config)

    def hybrid_search(self, *,
                      request: HybridSearchRequest,
                      partition_key: Dict[str, Any] = None,
                      projections: List[str] = None,
                      read_consistency: ReadConsistency = ReadConsistency.EVENTUAL,
                      config: Dict[Any, Any] = None):
        """hybrid search"""
        if not isinstance(request, HybridSearchRequest):
            raise ValueError("wrong type of argument 'request'")

        return self._search(request=request,
                            partition_key=partition_key,
                            projections=projections,
                            read_consistency=read_consistency,
                            config=config)

    def _search(self, *,
                request: SearchRequest,
                partition_key: Dict[str, Any] = None,
                projections: List[str] = None,
                read_consistency: ReadConsistency = ReadConsistency.EVENTUAL,
                config: Dict[Any, Any] = None):
        """internal use only"""
        if not self.conn:
            raise ClientError('conn is closed')

        body = request.to_dict()
        body["database"] = self.database_name
        body["table"] = self.table_name

        if partition_key is not None:
            body["partitionKey"] = partition_key
        if projections is not None:
            body["projections"] = projections
        body["readConsistency"] = read_consistency
        json_body = orjson.dumps(body)

        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'row')

        req_type = bytes(request.type())
        return self.conn.send_request(http_methods.POST,
                                      path=uri,
                                      body=json_body,
                                      params={req_type: b''},
                                      config=config)

    def delete(self, primary_key=None, partition_key=None, filter=None, config=None):
        """
        delete row
        """
        if not self.conn:
            raise ClientError('conn is closed')

        if primary_key is None and filter is None:
            raise ValueError('requiring primary_key or filter')
        if primary_key is not None and filter is not None:
            raise ValueError('only one of primary_key and filter should exist')
        if partition_key is not None and filter is not None:
            raise ValueError('only one of partition_key and filter should exist')

        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        if primary_key is not None:
            body["primaryKey"] = primary_key
        if partition_key is not None:
            body["partitionKey"] = partition_key
        if filter is not None:
            body["filter"] = filter
        json_body = orjson.dumps(body)

        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'row')

        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={bytes(RequestType.DELETE): b''},
                config=config)

    def update(self, primary_key=None, partition_key=None, update_fields=None, config=None):
        """
        update row
        """
        if not self.conn:
            raise ClientError('conn is closed')

        if primary_key is None and update_fields is None:
            raise ValueError('requiring primary_key and update_fields')

        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        if primary_key is not None:
            body["primaryKey"] = primary_key
        if partition_key is not None:
            body["partitionKey"] = partition_key
        if update_fields is not None:
            body["update"] = update_fields
        json_body = orjson.dumps(body)

        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'row')

        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={bytes(RequestType.UPDATE): b''},
                config=config)

    def select(self, filter=None, marker=None, projections=None, read_consistency=ReadConsistency.EVENTUAL, limit=10,
            config=None):
        """
        select
        """
        if not self.conn:
            raise ClientError('conn is closed')

        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        body["readConsistency"] = read_consistency
        body["limit"] = limit
        if filter is not None:
            body["filter"] = filter
        if marker is not None:
            body["marker"] = marker
        if projections is not None:
            body["projections"] = projections
        json_body = orjson.dumps(body)

        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'row')

        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={bytes(RequestType.SELECT): b''},
                config=config)

    @utils.deprecated("Use vector_search() with VectorBatchSearchRequest instead")
    def batch_search(self, anns, partition_key=None, projections=None, 
            retrieve_vector=False, read_consistency=ReadConsistency.EVENTUAL, 
            config=None):
        """
        Deprecated. Use vector_search() with VectorBatchSearchRequest instead.
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
                params={bytes(RequestType.BATCH_SEARCH): b''},
                config=config)
    
    def add_fields(self, schema, config=None):
        """
        add_fields
        """
        if not self.conn:
            raise ClientError('conn is closed')

        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        body["schema"] = schema.to_dict()
        json_body = orjson.dumps(body)

        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'table')

        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={bytes(RequestType.ADD_FIELD): b''},
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
                params={bytes(RequestType.CREATE): b''},
                config=config)

    def modify_index(self, index_name, auto_build, auto_build_index_policy=DefaultAutoBuildPolicy, config=None):
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
        if auto_build:
            body["index"]["autoBuildPolicy"] = auto_build_index_policy.to_dict()
        json_body = orjson.dumps(body)

        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'index')

        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={bytes(RequestType.MODIFY): b''},
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
                params={bytes(RequestType.REBUILD): b''},
                config=config)

    def describe_index(self, index_name, config=None):
        """describe index"""
        if not self.conn:
            raise ClientError('conn is closed')

        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'index')

        response = self.conn.send_request(http_methods.POST,
                path=uri,
                params={bytes(RequestType.DESC): b''},
                body=orjson.dumps({
                    'database': self.database_name,
                    'table': self.table_name,
                    'indexName': index_name
                }),
                config=config)
        index = response.index
        auto_build_index_policy = None
        if "autoBuildPolicy" in index:
            auto_build_index_policy = AutoBuildTool.get_auto_build_index_policy(index["autoBuildPolicy"])
        if index["indexType"] == IndexType.HNSW.value:
            return VectorIndex(
                index_name=index["indexName"],
                index_type=IndexType.HNSW,
                field=index["field"],
                metric_type=getattr(MetricType, index["metricType"], None),
                params=HNSWParams(m=index["params"]["M"],
                    efconstruction=index["params"]["efConstruction"]),
                auto_build=index["autoBuild"],
                auto_build_index_policy=auto_build_index_policy,
                state=getattr(IndexState, index["state"], None))
        elif index["indexType"] == IndexType.HNSWPQ.value:
            return VectorIndex(
                index_name=index["indexName"],
                index_type=IndexType.HNSWPQ,
                field=index["field"],
                metric_type=getattr(MetricType, index["metricType"], None),
                params=HNSWPQParams(m=index["params"]["M"],
                    efconstruction=index["params"]["efConstruction"],
                    NSQ=index["params"]["NSQ"],
                    samplerate=index["params"]["sampleRate"]),
                auto_build=index["autoBuild"],
                auto_build_index_policy=auto_build_index_policy,
                state=getattr(IndexState, index["state"], None))
        elif index["indexType"] == IndexType.FLAT.value:
            return VectorIndex(
                index_name=index["indexName"],
                index_type=IndexType.FLAT,
                field=index["field"],
                metric_type=getattr(MetricType, index["metricType"], None),
                auto_build=index["autoBuild"],
                auto_build_index_policy=auto_build_index_policy,
                state=getattr(IndexState, index["state"], None))
        elif index["indexType"] == IndexType.PUCK.value:
            return VectorIndex(
                index_name=index["indexName"],
                index_type=IndexType.PUCK,
                field=index["field"],
                metric_type=getattr(MetricType, index["metricType"], None),
                params=PUCKParams(coarseClusterCount=index["params"]["coarseClusterCount"],
                        fineClusterCount=index["params"]["fineClusterCount"]),
                auto_build=index["autoBuild"],
                auto_build_index_policy=auto_build_index_policy,
                state=getattr(IndexState, index["state"], None))
        elif index["indexType"] == IndexType.SECONDARY_INDEX.value:
            return SecondaryIndex(
                index_name=index["indexName"],
                field=index["field"])
        elif index["indexType"] == IndexType.INVERTED_INDEX.value:
            return InvertedIndex(
                index_name=index["indexName"],
                fields=index["fields"],
                params=InvertedIndexParams(analyzer=getattr(InvertedIndexAnalyzer, index["params"]["analyzer"], None),
                                    parse_mode=getattr(InvertedIndexParseMode, index["params"]["parseMode"], None)))
        else:
            raise ClientError("not supported index type:%s" % (index["indexType"]))


    def stats(self, config=None):
        """show table stats"""
        if not self.conn:
            raise ClientError('conn is closed')

        body = {}
        body["database"] = self.database_name
        body["table"] = self.table_name
        json_body = orjson.dumps(body)

        config = self._merge_config(config)
        uri = utils.append_uri(client.URL_PREFIX, client.URL_VERSION, 'table')

        return self.conn.send_request(http_methods.POST,
                path=uri,
                body=json_body,
                params={bytes(RequestType.STATS): b''},
                config=config)


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


@utils.deprecated("Use VectorSearchConfig instead")
class HNSWSearchParams:
    "hnsw search params"

    def __init__(self, ef=None, distance_far=None, distance_near=None, limit=50,
            pruning=True):
        self._ef = ef
        self._distance_far = distance_far
        self._distance_near = distance_near
        self._limit = limit
        self._pruning = pruning

    def to_dict(self):
        """to dict"""
        res = {}
        if self._ef is not None:
            res['ef'] = self._ef
        if self._distance_far is not None:
            res['distanceFar'] = self._distance_far
        if self._distance_near is not None:
            res['distanceNear'] = self._distance_near
        res['limit'] = self._limit
        res['pruning'] = self._pruning
        return res

class HNSWPQSearchParams:
    "hnswpq search params"

    def __init__(self, ef=None, distance_far=None, distance_near=None, limit=50):
        self._ef = ef
        self._distance_far = distance_far
        self._distance_near = distance_near
        self._limit = limit

    def to_dict(self):
        """to dict"""
        res = {}
        if self._ef is not None:
            res['ef'] = self._ef
        if self._distance_far is not None:
            res['distanceFar'] = self._distance_far
        if self._distance_near is not None:
            res['distanceNear'] = self._distance_near
        res['limit'] = self._limit
        return res


@utils.deprecated("Use VectorSearchConfig instead")
class HNSWPQSearchParams:
    "hnswpq search params"

    def __init__(self, ef=None, distance_far=None, distance_near=None, limit=50):
        self._ef = ef
        self._distance_far = distance_far
        self._distance_near = distance_near
        self._limit = limit

    def to_dict(self):
        """to dict"""
        res = {}
        if self._ef is not None:
            res['ef'] = self._ef
        if self._distance_far is not None:
            res['distanceFar'] = self._distance_far
        if self._distance_near is not None:
            res['distanceNear'] = self._distance_near
        res['limit'] = self._limit
        return res


@utils.deprecated("Use VectorSearchConfig instead")
class PUCKSearchParams:
    "puck search params"

    def __init__(self, searchCoarseCount, limit=50) -> None:
        self._limit = limit
        self._searchCoarseCount = searchCoarseCount

    def to_dict(self):
        """to dict"""
        res = {}

        res['searchCoarseCount'] = self._searchCoarseCount
        res['limit'] = self._limit

        return res


@utils.deprecated("Use VectorSearchConfig instead")
class FLATSearchParams:
    "flat search params"

    def __init__(self, distance_far=None, distance_near=None, limit=50):
        self._distance_far = distance_far
        self._distance_near = distance_near
        self._limit = limit

    def to_dict(self):
        """to dict"""
        res = {}
        if self._distance_far is not None:
            res['distanceFar'] = self._distance_far
        if self._distance_near is not None:
            res['distanceNear'] = self._distance_near
        res['limit'] = self._limit
        return res
