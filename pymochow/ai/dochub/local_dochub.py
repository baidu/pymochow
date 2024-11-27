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
from typing import List

_logger = logging.getLogger(__name__)

class LocalDocumentHub(DocumentHub):
    """
    LocalDocumentHub: A concrete implementation of DocumentHub that manages documents locally.
    """

    def __init__(self, env: DocumentHubEnv):
        """
        Initialize the LocalDocumentHub with the environment configuration.

        Parameters:
        env (DocumentHubEnv): The environment configuration containing the local cache path.
        """
        self._env = env
        self._root_path = env.root_path()
        self._schema = "local://"
        if self._root_path.startswith(self._schema):
            # Strip the 'local://' prefix for subsequent path operations
            self._root_path = self._root_path[len(self._schema):]

        else:
            raise ValueError(f"Invalid root_path format: {self._root_path}. Expected 'local://xxx/xxx'.")
        
        if not self._root_path.startswith("/"):
            self._root_path = "/" + self._root_path

        # Ensure the local cache directory exists
        if not os.path.exists(self._root_path):
            os.makedirs(self._root_path)

    def add(self, doc: Document) -> Document:
        """
        Add a document to the hub.

        Parameters:
        doc (Document): The document to be added to hub.
        """
        if doc.doc_name is None or doc.file_path is None:
            raise ValueError("Document file path or name is not set.")
        # Combine cache path with the document's directory structure
        target_path = os.path.join(self._root_path, doc.doc_name)

        # Ensure the target directory exists
        target_dir = os.path.dirname(target_path)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        
        # Copy the document to the target path
        shutil.copy(doc.file_path, target_path)
        
        _, file_extension = os.path.splitext(doc.doc_name)
        doc.doc_type = file_extension.lstrip(".")  # Remove the dot from extension

        # Update the document's file path, size, and creation time
        doc.file_path = target_path
        doc.uri = self._schema + target_path
        doc.size = os.path.getsize(target_path)  # Get file size
        doc.ctime = int(os.path.getctime(target_path))  # Get creation time
        _logger.info(f"Document '{doc.doc_name}' has been added to hub. path: {target_dir}.")
        return doc
        
    def remove(self, doc: Document):
        """
        Remove a document from the hub.

        Parameters:
        doc (Document): The document to be removed from hub.
        """
        target_path = os.path.join(self._root_path, doc.doc_name)
        if os.path.exists(target_path):
            os.remove(target_path)
            _logger.info(f"Document '{doc.doc_name}' has been removed from hub.")

            target_dir = os.path.dirname(target_path)
            
            while target_dir != self._root_path:
                if os.path.isdir(target_dir) and not os.listdir(target_dir):  # Directory is empty
                    os.rmdir(target_dir)
                    _logger.info(f"Directory '{target_dir}' has been removed as it is empty.")
                    target_dir = os.path.dirname(target_dir)  # Move up to the next parent directory
                else:
                    break
        else:
            _logger.info(f"Document '{doc.doc_name}' does not exist in hub.")

    def list(self) -> List[Document]:
        """
        List all documents in hub.

        Returns:
        List[Document]: A list of all documents in the hub.
        """
        documents = []
        for root, dirs, files in os.walk(self._root_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                relative_path = os.path.relpath(file_path, self._root_path)
                # Placeholder values for doc attributes that are not stored in the file system.
                _, file_extension = os.path.splitext(filename)
                doc_type = file_extension.lstrip(".")
                
                doc = Document(
                    doc_name=relative_path, 
                    doc_type=doc_type,
                    file_path=file_path, 
                    uri=self._schema + file_path,
                    size=os.path.getsize(file_path),
                    ctime=int(os.path.getctime(file_path))
                )
                documents.append(doc)
        return documents

    def load(self, doc: Document) -> Document:
        """
        Load a document if it in hub.

        Parameters:
        doc (Document): The document to load.

        Returns:
        doc (Document): If the document in hub then load it.
        """
        target_path = os.path.join(self._root_path, doc.doc_name)
        
        if os.path.exists(target_path):
            # Load the document's metadata (size and ctime) from the cached file
            doc.file_path = target_path
            doc.uri = self._schema + target_path
            doc.size = os.path.getsize(target_path)
            doc.ctime = int(os.path.getctime(target_path))
            
            _, file_extension = os.path.splitext(doc.doc_name)
            doc.doc_type = file_extension.lstrip(".")

            _logger.info(f"Document '{doc.doc_name}' loaded from hub.")
            return doc
        else:
            # If the document is not cached, return None or raise an error
            _logger.info(f"Document '{doc.doc_name}' is not in hub.")
            return None
