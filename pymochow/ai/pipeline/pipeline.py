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
This module provide interface for pipeline.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List
from pymochow.ai.embedder import Embedder
from pymochow.model.table import (
    Table,
    VectorSearchRequest,
    BM25SearchRequest,
    HybridSearchRequest
)
from pymochow.model.enum import ReadConsistency

class Pipeline(ABC):
    """
    Pipeline: An abstract base class that defines the interface for ingesting
    doc into vectordb and search from it.
    """
    @abstractmethod
    def ingest_doc(doc, 
            doc_processor=None, 
            embedder=None, 
            meta_table=None,
            doc_to_row_mapping=None,
            chunk_table=None,
            chunk_to_row_mapping=None):
        """
        Abstract method for processing and storing the ingestion of documents and their chunks.
        
        Parameters:
            doc (Document): The document object to be ingested.
            doc_processor (Processor, optional): A tool used to parse and split document.
            embedder (Embedder, optional): A tool used to generate embeddings of the document content.
            meta_table (Table, optional): The table in the database that stores document data.
            doc_to_row_mapping (dict, optional): A JSON-like dictionary that defines 
                the mapping between document object attributes and database table columns.
                Example mapping:
                {
                    'doc_id': 'document_id',  # Maps 'doc_id' in the document to 'document_id' in the database
                    'doc_name': 'document_name'  # Maps 'doc_name' in the document to 'document_name' in the database
                    # Add more mappings as needed...
                }
            chunk_table (Table, optional): The table in the database used to store document chunks, 
                if the document is processed in chunks.
            chunk_to_row_mapping (dict, optional): Similar to doc_to_row_mapping, 
                this dictionary defines how attributes of chunks map to database table columns.
                Example:
                {
                    'chunk_id': 'chunk_id',  # Example mapping, no change
                    # Additional mappings can be added here...
                }

        Returns:
            None: This method does not return anything but may modify data in the database or other storage systems.

        Note:
            This is an abstract method that must be implemented in any subclass inheriting 
             from Pipeline with specific logic for ingestion.
        """
        pass

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
        pass

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
        pass

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
        pass
