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
This module provide schema.
"""
from pymochow.model.enum import IndexType


class Field:
    """field"""

    def __init__(self, 
            field_name, 
            field_type, 
            primary_key=False, 
            partition_key=False,
            auto_increment=False,
            not_null=False,
            dimension=0):
        self._field_name = field_name
        self._field_type = field_type
        self._primary_key = primary_key
        self._partition_key = partition_key
        self._auto_increment = auto_increment
        self._not_null = not_null
        self._dimension = dimension

    @property
    def field_name(self):
        """field name"""
        return self._field_name

    @property
    def field_type(self):
        """field type"""
        return self._field_type

    @property
    def primary_key(self):
        """primary key"""
        return self._primary_key;

    @property
    def partition_key(self):
        """partition key"""
        return self._partition_key

    @property
    def auto_increment(self):
        """auto increment"""
        return self._auto_increment

    @property
    def not_null(self):
        """not null"""
        return self._not_null

    @property
    def dimension(self):
        """dimension"""
        return self._dimension

    def to_dict(self):
        """to dict"""
        res = {
            "fieldName": self.field_name,
            "fieldType": self.field_type
        }
        if self.primary_key:
            res["primaryKey"] = True
        if self.partition_key:
            res["partitionKey"] = True
        if self.auto_increment:
            res["autoIncrement"] = True
        if self.not_null:
            res["notNull"] = True
        if self.dimension > 0:
            res["dimension"] = self.dimension
        return res
    

class IndexField:
    """index field"""

    def __init__(self, index_name, field, index_type):
        self._index_name = index_name
        self._field = field
        self._index_type = index_type

    @property
    def index_type(self):
        """index type"""
        return self._index_type

    @property
    def index_name(self):
        """index name"""
        return self._index_name
    
    @property
    def field(self):
        """field"""
        return self._field


class HNSWParams:
    """
    The hnsw vector index params.
    """

    def __init__(self, m: int, efconstruction: int) -> None:
        self.m = m
        self.ef_construction = efconstruction
    
    def to_dict(self):
        """to dict"""
        res = {
            "M": self.m,
            "efConstruction": self.ef_construction
        }
        return res


class VectorIndex(IndexField):
    """
    Args:
        index_name(str): The field name of the index.
        field(str): make index on which field
        metric_type(MetricType): The metric type of the vector index.
        params(Any): HNSWParams if the index_type is HNSW
        auto_build(boolean): auto build vector index
    """
    def __init__(
            self,
            index_name,
            index_type,
            field,
            metric_type,
            params=None,
            auto_build=True,
            **kwargs):
        super().__init__(index_name=index_name, index_type=index_type, field=field)
        self._metric_type = metric_type
        self._params = params
        self._auto_build = auto_build
        self._state = kwargs.get('state', None)

    @property
    def metric_type(self):
        """metric type"""
        return self._metric_type

    @property
    def params(self):
        """params"""
        return self._params

    @property
    def auto_build(self):
        """auto build"""
        return self._auto_build
    
    @property
    def state(self):
        """state"""
        return self._state
    
    def to_dict(self):
        """to dict"""
        res = {
            "indexName": self.index_name,
            "indexType": self.index_type,
            "field": self.field,
            "metricType": self.metric_type,
            "autoBuild": self.auto_build
        }
        if self.params is not None:
            res["params"] = self.params.to_dict()
        if self.state is not None:
            res["state"] = self.state
        return res


class SecondaryIndex(IndexField):
    """secondary index"""

    def __init__(
            self,
            index_name,
            field):
        super().__init__(index_name=index_name, index_type=IndexType.SECONDARY_INDEX, 
                field=field)

    def to_dict(self):
        """to dict"""
        res = {
            "indexName": self.index_name,
            "indexType": self.index_type,
            "field": self.field
        }
        return res


class Schema:
    """schema"""

    def __init__(self, fields, indexes):
        self._indexes = indexes
        self._fields = fields

    @property
    def indexes(self):
        """indexes"""
        return self._indexes

    @property
    def fields(self):
        """fields"""
        return self._fields

    def to_dict(self):
        """to dict"""
        res = {}
        res["fields"] = []
        for field in self.fields:
            res["fields"].append(field.to_dict())
        
        res["indexes"] = []
        for index in self.indexes:
            res["indexes"].append(index.to_dict())
        return res
