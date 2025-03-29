"""
Query processing for the search engine component.
"""
from typing import List, Dict, Any
import time
import logging

from ..parser.text_normalizer import TextNormalizer
from ..utils.file_io import FileIO
from ..utils.compression import Compression


logger = logging.getLogger(__name__)


class QueryProcessor:
    """Processes search queries and retrieves results."""
    
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
        
        # Paths for index files
        self.isam_root_path = f"{index_dir}/isam_root.bin"
        self.isam_leaves_path = f"{index_dir}/isam_leaves.bin"
        self.postings_path = f"{index_dir}/postings.bin"
        
        # Load root index into memory
        self.root_index = FileIO.read_root_from_file(self.isam_root_path)
        
        # Open file handles
        self.leaves_file = open(self.isam_leaves_path, 'rb')
        self.postings_file = open(self.postings_path, 'rb')
    
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
        
        # Parse and normalize query
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
            postings = self._get_postings_for_term(term)
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
        
        # Store total results for reporting
        self.total_results = len(results)
        
        # Limit results only if max_results is specified
        if max_results is not None:
            results = results[:max_results]
        
        end_time = time.time()
        search_time = end_time - start_time
        
        logger.info(f"Search completed in {search_time:.3f} seconds")
        logger.info(f"Found {len(results)} results")
        
        # Validate search time requirement
        if search_time >= 1.0:
            logger.warning(
                f"Search time ({search_time:.3f}s) exceeds requirement of <1.0s"
            )
        
        return results
    
    def _get_postings_for_term(self, term: str) -> List[Dict[str, Any]]:
        """
        Get postings for a term.
        
        Args:
            term: Term to look up
            
        Returns:
            List of postings for the term, or empty list if not found
        """
        if not term or len(term) == 0:
            logger.warning("Attempted to look up empty term")
            return []
            
        # Make sure term is normalized and lowercase
        term = term.lower().strip()
        
        logger.info(f"Looking for term '{term}' in dictionary")
        
        # Binary search the root index
        leaf_location = self._binary_search_isam_root(term)
        
        if leaf_location is None:
            logger.warning(f"Term '{term}' not found in root index")
            return []
        
        # Binary search the leaf
        dictionary_entry = self._binary_search_isam_leaf(leaf_location, term)
        
        if dictionary_entry is None:
            logger.warning(f"Term '{term}' not found in leaf block")
            return []
        
        logger.info(f"Found term '{term}' in dictionary at location {dictionary_entry['location']}")
        
        # Retrieve postings
        postings = self._read_postings(
            dictionary_entry["location"],
            dictionary_entry["length"]
        )
        
        logger.info(f"Retrieved {len(postings)} postings for term '{term}'")
        
        return postings
    
    def _binary_search_isam_root(self, term: str) -> int:
        """
        Binary search the root index for a term.
        
        Args:
            term: Term to search for
            
        Returns:
            Location of the leaf block, or None if not found
        """
        if not self.root_index:
            return None
        
        left = 0
        right = len(self.root_index) - 1
        
        while left <= right:
            mid = (left + right) // 2
            
            if self.root_index[mid]["term"] == term:
                return self.root_index[mid]["location"]
            elif self.root_index[mid]["term"] < term:
                left = mid + 1
            else:
                right = mid - 1
        
        # If term not found, return the leaf block location
        if right < 0:
            return self.root_index[0]["location"]
        elif left >= len(self.root_index):
            return self.root_index[-1]["location"]
        else:
            return self.root_index[right]["location"]
    
    def _binary_search_isam_leaf(self, leaf_location: int, term: str) -> Dict[str, Any]:
        """
        Binary search a leaf block for a term.
        
        Args:
            leaf_location: Location of the leaf block
            term: Term to search for
            
        Returns:
            Dictionary entry, or None if not found
        """
        # Read the leaf block
        self.leaves_file.seek(leaf_location)
        
        # Read block size
        block_size_bytes = self.leaves_file.read(4)
        if not block_size_bytes:
            return None
        
        import struct
        block_size = struct.unpack('!I', block_size_bytes)[0]
        
        # Read all entries in the block
        entries = []
        for _ in range(block_size):
            # Read term
            term_len = struct.unpack('!H', self.leaves_file.read(2))[0]
            entry_term = self.leaves_file.read(term_len).decode('utf-8')
            
            # Read length and location
            length = struct.unpack('!I', self.leaves_file.read(4))[0]
            location = struct.unpack('!Q', self.leaves_file.read(8))[0]
            
            entries.append({
                "term": entry_term,
                "length": length,
                "location": location
            })
        
        # Binary search within the block
        left = 0
        right = len(entries) - 1
        
        while left <= right:
            mid = (left + right) // 2
            
            if entries[mid]["term"] == term:
                return entries[mid]
            elif entries[mid]["term"] < term:
                left = mid + 1
            else:
                right = mid - 1
        
        return None
    
    def _read_postings(self, location: int, length: int) -> List[Dict[str, Any]]:
        """
        Read postings from the postings file.
        
        Args:
            location: Location in the postings file
            length: Length of the compressed postings data
            
        Returns:
            List of postings
        """
        self.postings_file.seek(location)
        compressed_data = self.postings_file.read(length)
        
        return Compression.decompress_postings(compressed_data)
    
    def _score_documents(self, term_postings: Dict[str, List[Dict[str, Any]]]) -> Dict[str, float]:
        """
        Score documents based on TF-IDF.
        
        Args:
            term_postings: Dictionary mapping terms to postings
            
        Returns:
            Dictionary mapping document IDs to scores
        """
        import math
        
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
        import math
        
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
        import math
        
        # Avoid division by zero
        if doc_freq == 0:
            return 0
        
        return math.log10(self.total_documents / doc_freq)
    
    def close(self):
        """Close open file handles."""
        self.leaves_file.close()
        self.postings_file.close()