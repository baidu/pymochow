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
This module provide interface for doc parser.
"""
from pymochow.model.enum import DocSplitMode
from pymochow.model.document import DocumentChunk
from abc import ABC, abstractmethod
from typing import List


class DocProcessor(ABC):
    """
    DocProcessor: An abstract base class that defines the interface for parsing and splitting documents into chunks.

    Subclasses must implement the `process_doc` method to parse and split the document based on specific criteria,
    such as page length, overlap length, and the number of pages to take.
    """
    @abstractmethod
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
        pass
