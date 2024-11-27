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
This module provide appbuilder implement to parser and split doc.
"""

import logging
from typing import List
from pymochow.ai.processor import DocProcessor
from pymochow.model.document import DocumentChunk
from pymochow.model.enum import (
    DocSplitMode,
    Lang
)
from appbuilder.core.message import Message
from appbuilder.core.components.doc_parser.doc_parser import DocParser
from appbuilder.core.components.doc_splitter.doc_splitter import DocSplitter as ABDocSplitter

_logger = logging.getLogger(__name__)

class QianfanDocProcessor(DocProcessor):
    """
    QianfanDocProcessor: A concrete implementation of the DocProcessor class. 
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

    def process_doc(self, doc) -> List[DocumentChunk]:
        """
        Parse and split the document into chunks based on the provided parameters.
        
        Parameters:
        ----------
        doc : Document
            The document to be parsed and split into chunks.
        
        Returns:
        -------
        List[DocumentChunk]
            A list of DocumentChunk objects, where each chunk represents a part of the parsed document.
        """
        if self._split_mode != DocSplitMode.PAGE:
            raise ValueError(f"Unsupported split mode: {self._split_mode}. Only DocSplitMode.PAGE is supported.")
        
        if doc.lang == Lang.ZH:
            # Chinese language: use Chinese separators
            separators = ["。", "，", "？", "！", "……", "\n"]
        elif doc.lang == Lang.EN:
            # English language: use English separators
            separators = [".", ",", "?", "!", "...", "\n"]
        else:
            # Unsupported language, raise an error
            raise ValueError(f"Unsupported language: {doc.lang}. Only Lang.ZH and Lang.EN are supported.")

        msg = Message(doc.file_path)
        parser = DocParser()
        parse_result = parser(msg, return_raw=True)
        _logger.debug(f"parse_result:{parse_result}")

        doc_splitter = ABDocSplitter(splitter_type="split_by_chunk",
                                     separators=separators,
                                     max_segment_length=self._maximum_page_length,
                                     overlap=self._page_overlap_length)
        
        split_result = doc_splitter(parse_result)
        _logger.debug(f"split_result:{split_result}")
        
        paragraphs = split_result.content.get('paragraphs', [])
        chunks = []
        i = 1
        for paragraph in paragraphs:
            chunk = DocumentChunk(kb_id=doc.kb_id, doc_id=doc.doc_id, doc_name=doc.doc_name)
            chunk.content = paragraph.get('text', '')
            chunk.content_len = len(chunk.content)
            chunk.sequence_number = i
            chunks.append(chunk)
            i += 1

            if self._maximum_pages_totake is not None and len(chunks) >= self._maximum_pages_totake:
                break
            
        return chunks
