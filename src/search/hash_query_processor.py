"""
Query processing using hash-based index.
"""
from typing import List, Dict, Any
import time
import logging
import math

from ..parser.text_normalizer import TextNormalizer
from ..indexer.hash_index import HashIndex


logger = logging.getLogger(__name__)


class HashQueryProcessor:
    """Processes search queries using a hash-based index."""
    
    def __init__(
        self, 
        index_dir: str = "index",
        normalizer: TextNormalizer = None,
        total_documents: int = 0
    ):
        """
        Initialize the query processor.
        
        Args:
            index_dir: Directory containing index files
            normalizer: Text normalizer for query processing
            total_documents: Total number of documents in the corpus
        """
        self.index_dir = index_dir
        self.normalizer = normalizer or TextNormalizer()
        self.total_documents = total_documents
        
        # Initialize hash index
        self.hash_index = HashIndex(index_dir)
        loaded = self.hash_index.load()
        
        if not loaded:
            logger.warning("Failed to load hash index. Queries may not return results.")
    
    def search(self, query_string: str, max_results: int = None) -> List[Dict[str, Any]]:
        """
        Search for documents matching the query.
        
        Args:
            query_string: Query string
            max_results: Maximum number of results to return
            
        Returns:
            List of result documents with scores
        """
        start_time = time.time()
        
        # Parse and normalize query - use the same normalization as during indexing
        normalized_query = self.normalizer.normalize_text(query_string)
        logger.info(f"Normalized query: '{normalized_query}'")
        
        query_terms = self.normalizer.tokenize(normalized_query)
        logger.info(f"Query terms after tokenization: {query_terms}")
        
        # Limit to 5 terms as per requirement
        query_terms = query_terms[:5]
        
        if not query_terms:
            logger.warning("Empty query after normalization")
            return []
        
        logger.info(f"Searching for query terms: {query_terms}")
        
        # Get postings for each term
        term_postings = {}
        for term in query_terms:
            logger.info(f"Looking up term: '{term}'")
            postings = self.hash_index.get_postings(term)
            if postings:
                logger.info(f"Found {len(postings)} postings for term '{term}'")
                term_postings[term] = postings
            else:
                logger.warning(f"No postings found for term '{term}'")
        
        if not term_postings:
            logger.warning("No matching terms found in the index")
            return []
            
        # Score documents
        document_scores = self._score_documents(term_postings)
        logger.info(f"Scored {len(document_scores)} documents")
        
        # Sort results by score
        results = []
        for docno, score in document_scores.items():
            results.append({"docno": docno, "score": score})
        
        # Sort by score in descending order
        results.sort(key=lambda x: x["score"], reverse=True)
        
        # Log the total number of results before limiting
        total_results = len(results)
        logger.info(f"Total matches before limiting: {total_results}")
        
        # Store total results for reporting
        self.total_results = total_results
        
        # Limit results only if max_results is specified
        if max_results is not None:
            limited_results = results[:max_results]
            logger.info(f"Returning {len(limited_results)} of {total_results} total matches")
            return limited_results
        else:
            logger.info(f"Returning all {total_results} matches")
            return results
        
        end_time = time.time()
        search_time = end_time - start_time
        
        logger.info(f"Search completed in {search_time:.3f} seconds")
        logger.info(f"Returning {len(limited_results)} of {total_results} total matches")
        
        # Validate search time requirement
        if search_time >= 1.0:
            logger.warning(
                f"Search time ({search_time:.3f}s) exceeds requirement of <1.0s"
            )
        
        return limited_results
    
    def _score_documents(self, term_postings: Dict[str, List[Dict[str, Any]]]) -> Dict[str, float]:
        """
        Score documents based on TF-IDF.
        
        Args:
            term_postings: Dictionary mapping terms to postings
            
        Returns:
            Dictionary mapping document IDs to scores
        """
        document_scores = {}
        
        for term, postings in term_postings.items():
            # Calculate IDF component
            doc_freq = len(postings)
            idf = self._calculate_idf(doc_freq)
            
            for posting in postings:
                docno = posting["docno"]
                tf = posting["frequency"]
                
                # TF-IDF calculation
                score = self._calculate_tf_idf(tf, idf)
                
                if docno not in document_scores:
                    document_scores[docno] = 0
                
                document_scores[docno] += score
        
        return document_scores
    
    def _calculate_tf_idf(self, tf: int, idf: float) -> float:
        """
        Calculate TF-IDF score.
        
        Args:
            tf: Term frequency
            idf: Inverse document frequency
            
        Returns:
            TF-IDF score
        """
        # Using log normalization for TF
        if tf > 0:
            tf_normalized = 1 + math.log10(tf)
        else:
            tf_normalized = 0
        
        return tf_normalized * idf
    
    def _calculate_idf(self, doc_freq: int) -> float:
        """
        Calculate inverse document frequency.
        
        Args:
            doc_freq: Document frequency
            
        Returns:
            IDF value
        """
        # Avoid division by zero
        if doc_freq == 0:
            return 0
        
        return math.log10(self.total_documents / doc_freq)
    
    def get_vocabulary(self, prefix: str = None, limit: int = 100) -> List[str]:
        """
        Get a list of terms in the vocabulary.
        
        Args:
            prefix: Optional prefix to filter terms
            limit: Maximum number of terms to return
            
        Returns:
            List of terms
        """
        return self.hash_index.get_terms(prefix, limit)