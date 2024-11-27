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
This module provide appbuilder implement to split doc.
"""
import logging
import uuid
import time
from typing import List
from pymochow.ai.splitter import DocSplitter
from pymochow.model.document import DocumentChunk
from pymochow.model.enum import (
    DocSplitMode,
    Lang
)
from langchain.text_splitter import RecursiveCharacterTextSplitter

_logger = logging.getLogger(__name__)

class LangchainDocSplitter(DocSplitter):
    """
    LangchainDocSplitter: A concrete implementation of the DocProcessor class. 
    This class is responsible to parser and split a document into chunks based on a given splitting mode.
    """

    def __init__(
        self, 
        split_mode=DocSplitMode.PAGE,
        maximum_page_length=800,
        page_overlap_length=200,
        maximum_pages_totake=None
    ):
        """
        Initialize the QianfanDocProcessor.
        split_mode : DocSplitMode
            The mode of splitting the document (e.g., by page). The default is to split by pages.
        
        maximum_page_length : int, optional
            The maximum length of a chunk in terms of 
            pages (or another unit depending on the split mode). Defaults to None.
        
        page_overlap_length : int, optional
            The number of overlapping units (pages, words, etc.) between consecutive chunks, 
            ensuring some content overlap. Defaults to None.
        
        maximum_pages_totake : int, optional
            The total number of pages (or another unit) to process from the document. 
            This acts as a limit on how much of the document is parsed. Defaults to None.
        """
        self._split_mode = split_mode
        self._maximum_page_length = maximum_page_length
        self._page_overlap_length = page_overlap_length
        self._maximum_pages_totake = maximum_pages_totake

    def split(self, chunk) -> List[DocumentChunk]:
        """
        split the document into chunks based on the provided parameters.
        
        Parameters:
        ----------
        chunk : DocumentChunk
            The chunk to be parsed and split into chunks.
        
        Returns:
        -------
        List[DocumentChunk]
            A list of DocumentChunk objects, where each chunk represents a part of the parsed document.
        """
        if self._split_mode != DocSplitMode.PAGE:
            raise ValueError(f"Unsupported split mode: {self._split_mode}. Only DocSplitMode.PAGE is supported.")
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=self._maximum_page_length,  # Maximum length of each chunk
            chunk_overlap=self._page_overlap_length  # Overlap between chunks
        )

        chunks = text_splitter.split_text(chunk.content)

        document_chunks = []
        for idx, content in enumerate(chunks):
            new_chunk = DocumentChunk(
                kb_id=chunk.kb_id,
                doc_id=chunk.doc_id,
                chunk_id=str(uuid.uuid4()),  # Generate a unique chunk ID
                doc_name=chunk.doc_name,
                sequence_number=idx + 1,  # Create a sequence number for the chunk
                content=content,
                content_len=len(content),
                ctime=int(time.time())  # Use current time for chunk creation time
            )
            document_chunks.append(new_chunk)
            
            if self._maximum_pages_totake is not None and len(chunks) >= self._maximum_pages_totake:
                break

        return document_chunks
