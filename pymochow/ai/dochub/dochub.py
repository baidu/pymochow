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
This module provide interface for doc hub.
"""
from pymochow.model.document import Document
from abc import ABC, abstractmethod
from typing import List

# Abstract base class DocumentHub, defines the basic interface for a document hub
class DocumentHub(ABC):
    """
    DocumentHub: Abstract base class for the document hub that defines the basic operations.
    Subclasses should implement methods for adding, removing, listing, and downloading documents.
    """

    @abstractmethod
    def add(self, doc: Document) -> Document:
        """
        Add a document to the hub.

        Parameters:
        doc (Document): The document to be added.
        """
        pass

    @abstractmethod
    def remove(self, doc: Document):
        """
        Remove a document from the hub.

        Parameters:
        doc (Document): The document to be removed.
        """
        pass

    @abstractmethod
    def list(self) -> List[Document]:
        """
        List all documents available in the hub.

        Returns:
        List[Document]: A list of all available document objects.
        """
        pass

    @abstractmethod
    def load(self, doc: Document) -> Document:
        """
        Load a document from the hub.

        Parameters:
        doc (Document): The document to load.

        Returns:
        doc (Document): If the document in hub then load it.
        """
        pass


# Environment configuration class for DocumentHub
class DocumentHubEnv:
    """
    DocumentHubEnv: Stores configuration settings for interacting with an object storage service
    or a local file system. This class holds details like endpoint URLs, authentication keys,
    work path for object storage, and a local cache path.

    Parameters:
    ----------
    endpoint (str):
        The endpoint URL for the object storage service, such as an S3 or BOS-compatible service.
    ak (str):
        Access key for authentication with the object storage service.
    sk (str):
        Secret key for authentication with the object storage service.
    root_path (str):
        The root path where documents are stored. For object storage, this should be in the format
        'bos://bucket_name/object_prefix'. For local file systems, this should be in the format
        'local://root_path/'.
    local_cache_path (str):
        The local file path where downloaded documents will be cached. This parameter is only relevant
        when documents are stored in object storage (like BOS). It defines the location where documents
        are cached for local access.
    """

    def __init__(self, endpoint=None, ak=None, sk=None,
            root_path=None, local_cache_path=None):
        self._endpoint = endpoint
        self._access_key = ak
        self._secret_key = sk
        self._root_path = root_path
        self._local_cache_path = local_cache_path

    def endpoint(self):
        """
        Get the endpoint URL for the object storage service.

        Returns:
        str: The endpoint URL.
        """
        return self._endpoint

    def access_key(self):
        """
        Get the access key for the object storage service.

        Returns:
        str: The access key.
        """
        return self._access_key

    def secret_key(self):
        """
        Get the secret key for the object storage service.

        Returns:
        str: The secret key.
        """
        return self._secret_key

    def root_path(self):
        """
        Get the root path where documents are stored. 

        Returns:
        -------
        str: 
            The root path. For object storage, it would be in the format 'bos://bucket_name/object_prefix'. 
            For local file systems, it would be in the format 'local://root_path/'.
        """
        return self._root_path

    def local_cache_path(self):
        """
        Get the local cache path where downloaded documents are stored for quick access.
        This is only used when the documents are stored in object storage.

        Returns:
        -------
        str:
            The local file path where documents will be cached.
        """
        return self._local_cache_path
