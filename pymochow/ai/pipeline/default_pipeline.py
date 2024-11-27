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
This module provide implement for pipeline.
"""
import logging
from pymochow.ai.pipeline import Pipeline
from typing import Any, Dict, List
from pymochow.ai.embedder import Embedder
from pymochow.model.table import (
    Table,
    FloatVector,
    VectorSearchRequest,
    VectorTopkSearchRequest,
    VectorRangeSearchRequest,
    VectorBatchSearchRequest,
    BM25SearchRequest,
    HybridSearchRequest
)
from pymochow.model.enum import ReadConsistency

_logger = logging.getLogger(__name__)

class DefaultPipeline(Pipeline):
    """
    Implement for Pipeline class.
    """
    def __init__(self, batch_size=100):
        """
        Initialize the DefaultPipeline instance.
        """
        self._batch_size = batch_size
    
    def ingest_doc(
        self,
        doc, 
        doc_processor=None, 
        embedder=None, 
        meta_table=None,
        doc_to_row_mapping=None,
        chunk_table=None,
        chunk_to_row_mapping=None
    ):
        """
        Process and store a document and its chunks into the specified database tables.
        
        Parameters:
            doc (Document): The document object to be ingested.
            doc_processor (Processor, optional): A tool to parse and split document. 
            embedder (Embedder, optional): A tool to generate embeddings of the document content.
            meta_table (Table, optional): The table in the database where the document data will be stored.
            doc_to_row_mapping (dict, optional): A dictionary that maps document object attributes to 
                database table columns for the document.
            chunk_table (Table, optional): The table in the database used to store document chunks.
            chunk_to_row_mapping (dict, optional): A dictionary that maps chunk object attributes to 
                database table columns for the chunks.
        """
        if doc_processor is None:
            raise ValueError("doc_processor must be provided and non-None")
        if embedder is None:
            raise ValueError("embedder must be provided and non-None")
        if meta_table is None:
            raise ValueError("meta_table must be provided and non-None")
        if chunk_table is None:
            raise ValueError("chunk_table must be provided and non-None")

        rows = []
        rows.append(doc.to_row(doc_to_row_mapping))
        meta_table.insert(rows=rows)

        rows = []
        chunks = embedder.embedding(doc_processor.process_doc(doc=doc))
        for chunk in chunks:
            rows.append(chunk.to_row(chunk_to_row_mapping))
        for i in range(0, len(rows), self._batch_size):
            batch_rows = rows[i:i + self._batch_size]
            chunk_table.insert(rows=batch_rows)

    def vector_search(
        self, 
        search_contents: List[str], 
        embedder: Embedder, 
        table: Table, 
        search_request: VectorSearchRequest,
        partition_key: Dict[str, Any] = None,
        projections: List[str] = None,
        read_consistency: ReadConsistency = ReadConsistency.EVENTUAL,
        config: Dict[Any, Any] = None
    ):
        """
        Perform a vector-based search operation.

        This method converts the search content into a vector using the embedder and performs a
        vector search in the specified table, returning the most similar results to the query.

        Parameters:
        ----------
        search_contents : List[str]
            The input search content, usually in the form of text or already embedded vector.
        embedder : Embedder
            The embedder object used to convert the search content into vector form.
        table : Table
            The target table where the search is conducted.
        search_request : VectorSearchRequest
            The search request object containing parameters like TopK, filters, etc.
        partition_key : Dict[str, Any], optional
            The partition key to narrow the search to a specific partition, default is None.
        projections : List[str], optional
            The list of fields to include in the search result, default is None.
        read_consistency : ReadConsistency, optional
            The level of read consistency required, default is EVENTUAL.
        config : Dict[Any, Any], optional
            Additional configurations for the search, default is None.
        """
        if not search_contents or len(search_contents) == 0:
            raise ValueError("search_contents must not be None or empty")

        embeddings = embedder.embedding_text(search_contents)
        _logger.debug("embeddings result: {}".format(embeddings))

        if len(search_contents) == 1:
            if not isinstance(search_request, (VectorTopkSearchRequest, VectorRangeSearchRequest)):
                raise ValueError("For a single search content, search_request must be \
                        VectorTopkSearchRequest or VectorRangeSearchRequest")
            # embeddings[0] is the first and only embedding
            search_request.vector = FloatVector(embeddings[0])

        else:
            if not isinstance(search_request, VectorBatchSearchRequest):
                raise ValueError("For multiple search contents, search_request must be VectorBatchSearchRequest")

            search_request.vectors = [FloatVector(embedding) for embedding in embeddings]
        
        _logger.debug("search_request: {}".format(search_request.to_dict()))

        result = table.vector_search(
            request=search_request,
            partition_key=partition_key,
            projections=projections,
            read_consistency=read_consistency,
            config=config
        )

        return result

    def bm25_search(
        self,
        table: Table,
        search_request: BM25SearchRequest,
        partition_key: Dict[str, Any] = None,
        projections: List[str] = None,
        read_consistency: ReadConsistency = ReadConsistency.EVENTUAL,
        config: Dict[Any, Any] = None
    ):
        """
        Perform a BM25-based text search operation.

        This method uses the BM25 search algorithm to perform a text-based search on the specified
        table, returning the most relevant documents based on the query terms.

        Parameters:
        ----------
        table : Table
            The target table where the search will be performed.
        search_request : BM25SearchRequest
            The search request object, which contains query terms and other parameters.
        partition_key : Dict[str, Any], optional
            The partition key to narrow the search to a specific partition, default is None.
        projections : List[str], optional
            The list of fields to include in the search result, default is None.
        read_consistency : ReadConsistency, optional
            The level of read consistency required, default is EVENTUAL.
        config : Dict[Any, Any], optional
            Additional configurations for the search, default is None.
        """
        result = table.bm25_search(
            request=search_request, 
            partition_key=partition_key,
            projections=projections,
            read_consistency=read_consistency,
            config=config
        )
        return result

    def hybrid_search(
        self, 
        search_contents: List[str], 
        embedder: Embedder, 
        table: Table, 
        search_request: HybridSearchRequest,
        partition_key: Dict[str, Any] = None,
        projections: List[str] = None,
        read_consistency: ReadConsistency = ReadConsistency.EVENTUAL,
        config: Dict[Any, Any] = None
    ):
        """
        Perform a hybrid search (vector + traditional text search).

        This method combines vector search with BM25 text search, suitable for scenarios
        requiring both semantic and keyword-based search.

        Parameters:
        ----------
        search_contents : List[str]
            The input search content, usually text-based.
        embedder : Embedder
            The embedder object used to convert the search content into vectors.
        table : Table
            The target table where the search is conducted.
        partition_key : Dict[str, Any], optional
            The partition key to narrow the search to a specific partition, default is None.
        projections : List[str], optional
            The list of fields to include in the search result, default is None.
        read_consistency : ReadConsistency, optional
            The level of read consistency required, default is EVENTUAL.
        config : Dict[Any, Any], optional
            Additional configurations for the search, default is None.
        """
        if not search_contents or len(search_contents) == 0:
            raise ValueError("search_contents must not be None or empty")
        
        embeddings = embedder.embedding_text(search_contents)

        if len(search_contents) == 1:
            if not isinstance(search_request._vector_request, (VectorTopkSearchRequest, VectorRangeSearchRequest)):
                raise ValueError("For a single search content, search_request \
                        must be VectorTopkSearchRequest or VectorRangeSearchRequest")
            # embeddings[0] is the first and only embedding
            search_request._vector_request.vector = FloatVector(embeddings[0])

        else:
            if not isinstance(search_request._vector_request, VectorBatchSearchRequest):
                raise ValueError("For multiple search contents, search_request must be VectorBatchSearchRequest")

            search_request._vector_request.vectors = [FloatVector(embedding) for embedding in embeddings]

        result = table.hybrid_search(
            request=search_request,
            partition_key=partition_key,
            projections=projections,
            read_consistency=read_consistency,
            config=config
        )

        return result
