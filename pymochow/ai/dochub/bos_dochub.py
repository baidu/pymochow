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
This module provide local document hub.
"""
from pymochow.model.document import Document
from pymochow.ai.dochub import DocumentHub, DocumentHubEnv
import logging
import os
import shutil
import time
from datetime import datetime
from typing import List
from baidubce.services.bos.bos_client import BosClient
from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials
from baidubce.exception import BceHttpClientError

_logger = logging.getLogger(__name__)

class BosDocumentHub(DocumentHub):
    """
    BOSDocumentHub: A concrete implementation of DocumentHub that manages documents in Baidu Object Storage (BOS).
    """

    def __init__(self, env: DocumentHubEnv):
        """
        Initialize the BOSDocumentHub with credentials and region information.
        """
        self._env = env
        if not env.endpoint() or not env.access_key() or not env.secret_key() or not env.local_cache_path():
            raise ValueError("All parameters (endpoint, access key, secret key, local cache path) must be provided.")
        
        self._schema = "bos://"

        # Extract the bucket name and object prefix from the work path
        if env.root_path().startswith(self._schema):
            path_parts = env.root_path()[6:].split('/', 1)  # Strip 'bos://' and split into bucket and prefix
            self._bucket_name = path_parts[0]
            self._object_prefix = path_parts[1] if len(path_parts) > 1 else ''
        else:
            raise ValueError("Invalid root_path format for BOSDocumentHub. Expected 'bos://bucket_name/object_prefix'.")

        # Create BOS client
        self._client = self._create_bos_client(env.endpoint(), env.access_key(), env.secret_key())
    
    def _create_bos_client(self, endpoint, access_key, secret_key):
        """
        Create and configure the BOS client with the given credentials and endpoint.

        Parameters:
        endpoint (str): BOS service endpoint.
        access_key (str): Access key for BOS authentication.
        secret_key (str): Secret key for BOS authentication.

        Returns:
        BosClient: Configured client for interacting with Baidu Object Storage.
        """
        credentials = BceCredentials(access_key, secret_key)
        config = BceClientConfiguration(credentials, endpoint)
        return BosClient(config)
    
    def add(self, doc: Document) -> Document:
        """
        Upload a document to the BOS bucket.
        """
        object_key = f"{self._object_prefix}/{doc.doc_name}" if self._object_prefix else doc.doc_name
        self._client.put_object_from_file(self._bucket_name, object_key, doc.file_path)
        doc.uri = f"{self._schema}{self._bucket_name}/{object_key}"
        doc.size = os.path.getsize(doc.file_path)  # Get file size
        doc.ctime = int(time.time())
        return doc

    def remove(self, doc: Document):
        """
        Delete a document from the BOS bucket.
        """
        object_key = f"{self._object_prefix}/{doc.doc_name}" if self._object_prefix else doc.doc_name
        try:
            self._client.delete_object(self._bucket_name, object_key)
        except BceHttpClientError as e:
            if e.code == 'NoSuchKey':  # Check if the error code corresponds to a missing object
                _logger.info(f"Document '{doc.doc_name}' does not exist. No action needed.")
            else:
                raise  # Re-raise the exception if it is not a 'NoSuchKey' error
        except Exception as e:
            _logger.warning(f"Unexpected error occurred while deleting '{doc.doc_name}': {e}")
            raise

    def list(self) -> List[Document]:
        """
        List all documents stored in the BOS bucket.
        """
        response = self._client.list_objects(self._bucket_name, prefix=self._object_prefix)
        documents = []
        for obj in response.contents:
            if obj.key.endswith('/'):  # Skip directories
                continue
            doc_name = obj.key[len(self._object_prefix) + 1:] if self._object_prefix else obj.key
            doc_uri = f"{self._schema}{self._bucket_name}/{obj.key}"
            ctime = int(datetime.strptime(obj.last_modified, "%Y-%m-%dT%H:%M:%SZ").timestamp())
            
            documents.append(Document(
                doc_name=doc_name,
                uri=doc_uri,
                size=obj.size,  # Size in bytes
                ctime=ctime  # Creation time as a timestamp
            ))
        return documents

    def load(self, doc: Document) -> Document:
        """
        Load a document if it exists in the BOS bucket.
        """
        object_key = f"{self._object_prefix}/{doc.doc_name}" if self._object_prefix else doc.doc_name
        local_file_path = os.path.join(self._env.local_cache_path(), doc.doc_name)
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        response = self._client.get_object_meta_data(self._bucket_name, object_key)
        doc.size = response.metadata.content_length  # 更新 size
        doc.ctime = int(datetime.strptime(
            response.metadata.last_modified, "%a, %d %b %Y %H:%M:%S GMT"
        ).timestamp())

        self._client.get_object_to_file(self._bucket_name, object_key, local_file_path)
        doc.file_path = local_file_path
        doc.uri = f"{self._schema}{self._bucket_name}/{object_key}"
        return doc
