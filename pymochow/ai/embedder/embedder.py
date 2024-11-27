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
This module provide interface for embedding.
"""
from pymochow.model.document import DocumentChunk
from abc import ABC, abstractmethod
from typing import List

class Embedder(ABC):
    """
    Embedder: An abstract base class for generating embeddings for document chunks.
    """
    
    @abstractmethod
    def embedding(self, chunks) -> List[DocumentChunk]:
        """
        Generate embeddings for specified fields in document chunks.

        Parameters:
        chunks (List[DocumentChunk]): A list of document chunks that need to be processed for embeddings.
        Returns:
        List[DocumentChunk]: A list of `DocumentChunk` objects, 
            with the embeddings added to the corresponding fields based on `field_mapping`.
        """
        pass

    @abstractmethod
    def embedding_text(self, texts) -> List[List[float]]:
        """
        Embeds a given text into a numerical representation and returns the result.

        Args:
            text (List[str]): The text to be embedded.

        Returns:
            List[List[float]]: The numerical embedding of the text as a list of floats.

        Note:
            The method includes a sleep call to throttle the embedding rate due to API rate limits.
        """
