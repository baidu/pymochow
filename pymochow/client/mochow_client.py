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
This module provides a client class for Mochow
"""
import copy
import logging
from typing import List
from requests.adapters import HTTPAdapter

import pymochow
from pymochow.http.http_client import HTTPClient
from pymochow.model.database import Database
from pymochow.exception import ClientError
from pymochow import configuration

_logger = logging.getLogger(__name__)


class MochowClient:
    """
    mochow sdk client
    """

    def __init__(self, config=None, adapter: HTTPAdapter=None):
        self._config = copy.deepcopy(configuration.DEFAULT_CONFIG)
        if config is not None:
            self._config.merge_non_none_values(config)
        self._conn = HTTPClient(config, adapter)
    
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
    
    def create_database(self, database_name, config=None) -> Database:
        """create database
        Args:
            database_name(str): database name
            config (dict): config need merge
        Returns:
            Database：database
        """
        config = self._merge_config(config)
        db = Database(conn=self._conn, database_name=database_name, config=config)
        db.create_database()
        return db

    def list_databases(self, config=None) -> List[Database]:
        """list database
        Args:
            config (dict): config need merge
        Returns:
            List[Database]：database list
        """
        config = self._merge_config(config)
        db = Database(conn=self._conn, config=config)
        return db.list_databases()
    
    def database(self, database_name, config=None) -> Database:
        """get database
        Args:
            database_name(str): database name
            config (dict): config
        Returns:
            Database：database
        """
        for db in self.list_databases(config):
            if db.database_name == database_name:
                return db
        raise ClientError(message='Database not exist: {}'.format(database_name))

    def drop_database(self, database_name, config=None):
        """drop database
        Args:
            database_name(str): database name
            config (dict): config
        """
        db = self.database(database_name, config)
        db.drop_database()

    def close(self):
        """Close the connect session."""
        if self._conn:
            self._conn.close()
            self._conn = None
