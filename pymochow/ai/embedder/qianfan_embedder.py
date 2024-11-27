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
This module provide implement for qianfan embedding.
"""
import time
import logging
from typing import List
from pymochow.model.document import DocumentChunk
from pymochow.ai.embedder import Embedder
import appbuilder
from appbuilder import Message

_logger = logging.getLogger(__name__)

class QianfanEmbedder(Embedder):
    """
    QianfanEmbedder: Implementation of the Embedder class by Qianfan SDK. 
    """
    def __init__(self, model=None, batch=10, field_mapping=None):
        """
        Initialize the QianfanEmbedder.
        """
        if model is not None:
            self._embedding = appbuilder.Embedding(model)
        else:
            self._embedding = appbuilder.Embedding()

        if field_mapping is None:
            self._field_mapping = {'content': 'embedding'}
        else:
            self._field_mapping = field_mapping
        self._batch = batch

    def embedding(self, chunks) -> List[DocumentChunk]:
        """
        Generate embeddings for specified fields in document chunks.

        Parameters:
        chunks (List[DocumentChunk]): A list of document chunks that need to be processed for embeddings.
        Returns:
        List[DocumentChunk]: A list of `DocumentChunk` objects, 
            with the embeddings added to the corresponding fields based on `field_mapping`.
        """
        # Batch processing of document chunks for embeddings
        for i in range(0, len(chunks), self._batch):
            batch_chunks = chunks[i:i + self._batch]
            
            # Prepare data for each field that needs embedding
            for field, vector in self._field_mapping.items():
                field_data = [getattr(chunk, field) for chunk in batch_chunks]
                result = self._embedding.batch(Message(field_data))
                # Apply the embeddings to the corresponding vector field
                for chunk, embedding in zip(batch_chunks, result.content):
                    setattr(chunk, vector, embedding)

            time.sleep(1)  # Respect API rate limit

        return chunks
    
    def embedding_text(self, texts) -> List[List[float]]:
        """
        Processes a list of text strings and converts them into embeddings using a batch processing method.

        Parameters:
            texts (List[str]): A list of text strings to be embedded.

        Returns:
            List[List[float]]: A list of lists, where each inner list 
                is a numerical vector representing the embedded text.
        """
        embedding = []
        for i in range(0, len(texts), self._batch):
            batch_texts = texts[i:i + self._batch]
            result = self._embedding.batch(Message(batch_texts))
            embedding.extend(result.content)
        return embedding

