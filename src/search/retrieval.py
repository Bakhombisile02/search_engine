"""
Document retrieval functionality for the search engine.
"""
import os
import json
import logging
from typing import List, Dict, Any, Optional

from ..parser.models import Document
from .query_processor import QueryProcessor


logger = logging.getLogger(__name__)


class DocumentRetriever:
    """Retrieves document information for search results."""
    
    def __init__(self, document_store_path: str):
        """
        Initialize the document retriever.
        
        Args:
            document_store_path: Path to the document store file
        """
        self.document_store_path = document_store_path
        self.document_cache = {}
        
        # Check if document store exists
        if not os.path.exists(document_store_path):
            logger.warning(f"Document store not found: {document_store_path}")
    
    def get_document(self, docno: str) -> Optional[Document]:
        """
        Get a document by its document number.
        
        Args:
            docno: Document number
            
        Returns:
            Document object, or None if not found
        """
        # Check cache first
        if docno in self.document_cache:
            return self.document_cache[docno]
        
        # Try to load from document store
        try:
            with open(self.document_store_path, 'r', encoding='utf-8') as file:
                for line in file:
                    doc_data = json.loads(line.strip())
                    if doc_data.get('docno') == docno:
                        document = Document(
                            docno=doc_data.get('docno', ''),
                            docid=doc_data.get('docid', ''),
                            date=doc_data.get('date', ''),
                            headline=doc_data.get('headline', ''),
                            source=doc_data.get('source', ''),
                            content=doc_data.get('content', '')
                        )
                        
                        # Cache for future requests
                        self.document_cache[docno] = document
                        
                        return document
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Error retrieving document {docno}: {str(e)}")
        
        return None


class SearchEngine:
    """Main search engine class combining query processing and document retrieval."""
    
    def __init__(
        self,
        index_dir: str = "index",
        document_store_path: str = "data/document_store.jsonl",
        total_documents: int = 0
    ):
        """
        Initialize the search engine.
        
        Args:
            index_dir: Directory containing index files
            document_store_path: Path to the document store file
            total_documents: Total number of documents in the corpus
        """
        # Import the query processor based on index type (hash or ISAM)
        import os
        import json
        
        # Check for index_stats.json to determine index type
        stats_path = os.path.join(index_dir, "index_stats.json")
        index_type = "hash"  # Default to hash index
        
        if os.path.exists(stats_path):
            try:
                with open(stats_path, 'r', encoding='utf-8') as f:
                    stats = json.load(f)
                index_type = stats.get("index_type", "hash")
            except Exception as e:
                logger.warning(f"Error reading index stats: {str(e)}")
        
        # Create appropriate query processor
        if index_type == "hash":
            from .hash_query_processor import HashQueryProcessor
            self.query_processor = HashQueryProcessor(
                index_dir=index_dir,
                total_documents=total_documents
            )
            logger.info("Using hash-based query processor")
        else:
            from .query_processor import QueryProcessor
            self.query_processor = QueryProcessor(
                index_dir=index_dir,
                total_documents=total_documents
            )
            logger.info("Using ISAM-based query processor")
        
        self.document_retriever = DocumentRetriever(document_store_path)
    
    def search(
        self, 
        query_string: str, 
        max_results: int = None, 
        retrieve_documents: bool = False
    ) -> Dict[str, Any]:
        """
        Search for documents matching the query.
        
        Args:
            query_string: Query string
            max_results: Maximum number of results to return
            retrieve_documents: Whether to retrieve full documents
            
        Returns:
            Dictionary with search results
        """
        # Process query
        results = self.query_processor.search(query_string, max_results)
        
        # Retrieve documents if requested
        if retrieve_documents:
            for result in results:
                document = self.document_retriever.get_document(result['docno'])
                if document:
                    result['document'] = {
                        'docno': document.docno,
                        'docid': document.docid,
                        'date': document.date,
                        'headline': document.headline,
                        'source': document.source,
                        'content': document.content[:200] + '...' if len(document.content) > 200 else document.content
                    }
        
        # Try to get total results count if available
        total_results = getattr(self.query_processor, 'total_results', len(results))
        
        return {
            'query': query_string,
            'num_results': len(results),
            'total_results': total_results,
            'results': results
        }
    
    def close(self):
        """Close resources."""
        if hasattr(self.query_processor, 'close'):
            self.query_processor.close()