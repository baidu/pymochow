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
from pymochow.model.document import DocumentChunk
from abc import ABC, abstractmethod
from typing import List


class DocParser(ABC):
    """
    DocParser: An abstract base class that defines the interface for parsing  documents into chunks.

    Subclasses must implement the `process_doc` method to parse the document based on specific criteria,
    such as page length, overlap length, and the number of pages to take.
    """
    @abstractmethod
    def parse(self, doc) -> DocumentChunk:
        """
        Parse document into chunks based on the provided parameters.

        Parameters:
        ----------
        doc : Document
            The document to be parsed and split into chunks.
        
        Returns:
        -------
        DocumentChunk
            A DocumentChunk object.
        """
        pass
