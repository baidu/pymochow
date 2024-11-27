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
This module provide appbuilder implement to parser doc.
"""

import logging
import time
import os
import uuid
from typing import List
from pymochow.ai.parser import DocParser
from pymochow.model.document import DocumentChunk
from langchain_community.document_loaders import PyPDFLoader

_logger = logging.getLogger(__name__)

class LangchainDocParser(DocParser):
    """
    LangchainDocParser: A concrete implementation of the DocProcessor class. 
    This class is responsible to parser a document into chunks based on a given splitting mode.
    """

    def __init__(
        self
    ):
        """
        Initialize the QianfanDocProcessor.
        """
        pass

    def parse(self, doc) -> DocumentChunk:
        """
        Parse the document into chunk based on the provided parameters.
        
        Parameters:
        ----------
        doc : Document
            The document to be parsed and split into chunks.
        
        Returns:
        -------
        DocumentChunk
            A  DocumentChunk object
        """
        if doc.file_path is None:
            raise ValueError("Document does not have a file path.")
        
        file_extension = os.path.splitext(doc.file_path)[1].lower()
        if file_extension != '.pdf':
            raise ValueError("Only PDF files are supported. Provided file is not a PDF.")

        # Using PyPDFLoader to load the document (specifically for PDF files)
        loader = PyPDFLoader(doc.file_path)
        langchain_docs = loader.load()

        document_content = "\n".join([doc.page_content for doc in langchain_docs])

        chunk_obj = DocumentChunk(
            kb_id=doc.kb_id,
            doc_id=doc.doc_id,
            chunk_id=str(uuid.uuid4()),  # Generate a unique chunk ID
            doc_name=doc.doc_name,
            sequence_number=1,  # Since it's a single chunk, the sequence number is 1
            content=document_content,
            content_len=len(document_content),  # Set the length of the chunk content
            ctime=int(time.time())  # Use current time for chunk creation time
        )
        return chunk_obj
