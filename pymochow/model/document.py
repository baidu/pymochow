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
This module provide document model.
"""
import uuid
import time
from pymochow.model.table import Row
from pymochow.model.enum import (
    DocumentLayout,
    Lang
)

class Document:
    """Document Model"""
   
    def __init__(
        self,
        kb_id=None,
        doc_id=None,
        doc_name=None,
        doc_type=None,
        layout=DocumentLayout.GENERAL,
        lang=Lang.ZH,
        file_path=None,
        uri=None,
        size=None,
        ctime=None
    ):
        if not doc_name:
            raise ValueError("doc_name is required")  # Raise an error if doc_name is not provided
        
        self.doc_id = doc_id if doc_id is not None else str(uuid.uuid4())
        
        self.kb_id = kb_id
        self.doc_name = doc_name
        self.doc_type = doc_type # pdf/doc/markdown/excel
        self.layout = layout # Q&A/paper/law
        self.lang = lang # en/zh
        self.file_path = file_path # local file path
        self.uri = uri # uri in FileSystem, example: bos://bucket/xxx
        self.size = size
        self.ctime = int(ctime if ctime is not None else time.time())
    
    def to_row(self, field_mapping=None):
        """
        Convert Document to Row with custom field mapping.

        Parameters:
        ----------
        field_mapping: dict
            A dictionary mapping Document attributes to Row fields.

        Returns:
        -------
        Row
            A Row object created from the Document object with custom field names.
        """
        data = {}
        for field, value in self.__dict__.items():
            if value is None:
                continue

            # If field_mapping is None, convert all fields
            if field_mapping is None:
                new_field_name = field  # Keep original field name
            # If field_mapping exists, only convert fields that are in the mapping
            elif field in field_mapping:
                new_field_name = field_mapping.get(field, field)
            else:
                continue  # Skip fields that are not in field_mapping
            data[new_field_name] = value

        return Row(**data)

    def to_dict(self):
        """
        Convert the Document object to a dictionary representation.

        Returns:
        -------
        dict
            A dictionary representation of the Document object.
        """
        # Simply return the __dict__ attribute, which is a dictionary of all instance attributes
        return self.__dict__.copy()

class DocumentChunk:
    """Document Chunk Model"""

    def __init__(
        self, 
        kb_id=None,
        doc_id=None,
        chunk_id=None, 
        doc_name=None,
        sequence_number=None,
        content=None,
        content_len=None,
        embedding=None,
        ctime=None,
        **kwargs
    ):
        
        if not kb_id:
            raise ValueError("kb_id cannot be None")
        if not doc_id:
            raise ValueError("doc_id cannot be None")
        if not doc_name:
            raise ValueError("doc_name cannot be None")

        # If chunk_id is None, generate a UUID
        self.chunk_id = chunk_id if chunk_id is not None else str(uuid.uuid4())
        
        self.kb_id = kb_id
        self.doc_id = doc_id
        self.doc_name = doc_name
        self.sequence_number = sequence_number
        self.content = content
        self.content_len = content_len
        self.embedding = embedding
        self.ctime = int(ctime if ctime is not None else time.time())

        for key, value in kwargs.items():
            setattr(self, key, value)

    def to_row(self, field_mapping=None):
        """Convert DocumentChunk to Row with custom field mapping
        field_mapping = {
            'doc_id': 'document_id',  # example: doc_id -> document_id
            'chunk_id': 'chunk_id',   # no change, just for example
            'doc_name': 'document_name',  # doc_name -> document_name
            # Add more mappings as needed...
        }
        """
        data = {}
        for field, value in self.__dict__.items():
            if value is None:
                continue

            # If field_mapping is None, convert all fields
            if field_mapping is None:
                new_field_name = field  # Keep original field name
            # If field_mapping exists, only convert fields that are in the mapping
            elif field in field_mapping:
                new_field_name = field_mapping.get(field, field)
            else:
                continue  # Skip fields that are not in field_mapping
            data[new_field_name] = value

        return Row(**data)
    
    def to_dict(self):
        """
        Convert the DocumentChunk object to a dictionary representation.

        Returns:
        -------
        dict
            A dictionary representation of the DocumentChunk object.
        """
        # Simply return the __dict__ attribute, which is a dictionary of all instance attributes
        return self.__dict__.copy()
