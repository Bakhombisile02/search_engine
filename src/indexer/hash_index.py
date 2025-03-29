"""
Hash-based index structure for efficient term lookup.
"""
import os
import pickle
import logging
from typing import Dict, List, Any, Tuple
from collections import defaultdict

from ..utils.compression import Compression
from ..utils.file_io import FileIO

logger = logging.getLogger(__name__)

class HashIndex:
    """Hash-based index for mapping terms to postings."""
    
    def __init__(self, index_dir: str = "index"):
        """
        Initialize the hash index.
        
        Args:
            index_dir: Directory to store index files
        """
        self.index_dir = index_dir
        self.hash_table_path = os.path.join(index_dir, "hash_table.pkl")
        self.postings_path = os.path.join(index_dir, "postings.bin")
        
        # Ensure the directory exists
        os.makedirs(index_dir, exist_ok=True)
        
        # In-memory hash table for quick access
        self.hash_table = {}
    
    def build_index(self, term_doc_pairs: List[Tuple[str, str, int]]) -> Dict[str, Any]:
        """
        Build a hash-based index from term-document pairs.
        
        Args:
            term_doc_pairs: List of (term, docno, frequency) tuples
            
        Returns:
            Dictionary with indexing statistics
        """
        logger.info(f"Building hash index from {len(term_doc_pairs)} term-document pairs")
        
        # Group by term
        term_groups = defaultdict(list)
        for term, docno, frequency in term_doc_pairs:
            term_groups[term].append((docno, frequency))
        
        num_terms = len(term_groups)
        logger.info(f"Found {num_terms} unique terms")
        
        # Build the hash table and postings file
        with open(self.postings_path, 'wb') as postings_file:
            for term, postings in term_groups.items():
                # Write postings to file
                location = postings_file.tell()
                
                # Convert to required format for compression
                formatted_postings = [{"docno": docno, "frequency": freq} for docno, freq in postings]
                
                # Compress and write
                compressed_postings = Compression.compress_postings(formatted_postings)
                postings_file.write(compressed_postings)
                
                # Add to hash table
                self.hash_table[term] = {
                    "length": len(compressed_postings),
                    "location": location,
                    "doc_count": len(postings)
                }
        
        # Save the hash table
        self._save_hash_table()
        
        # Return statistics
        return {
            "num_terms": num_terms,
            "num_term_doc_pairs": len(term_doc_pairs),
            "hash_table_size": len(pickle.dumps(self.hash_table))
        }
    
    def _save_hash_table(self) -> None:
        """Save the hash table to disk."""
        logger.info(f"Saving hash table with {len(self.hash_table)} entries")
        FileIO.write_pickle(self.hash_table, self.hash_table_path)
    
    def load(self) -> bool:
        """
        Load the hash table from disk.
        
        Returns:
            True if loaded successfully, False otherwise
        """
        try:
            if os.path.exists(self.hash_table_path):
                self.hash_table = FileIO.read_pickle(self.hash_table_path)
                logger.info(f"Loaded hash table with {len(self.hash_table)} entries")
                return True
            else:
                logger.warning(f"Hash table file not found: {self.hash_table_path}")
                return False
        except Exception as e:
            logger.error(f"Error loading hash table: {str(e)}")
            return False
    
    def get_postings(self, term: str) -> List[Dict[str, Any]]:
        """
        Get postings for a term.
        
        Args:
            term: Term to look up
            
        Returns:
            List of postings for the term, or empty list if not found
        """
        # Look up in hash table
        entry = self.hash_table.get(term)
        
        if not entry:
            return []
        
        # Read postings from file
        with open(self.postings_path, 'rb') as f:
            f.seek(entry["location"])
            compressed_data = f.read(entry["length"])
        
        # Decompress postings
        return Compression.decompress_postings(compressed_data)
    
    def get_terms(self, prefix: str = None, limit: int = 100) -> List[str]:
        """
        Get terms from the hash table, optionally filtered by prefix.
        
        Args:
            prefix: Optional prefix to filter terms
            limit: Maximum number of terms to return
            
        Returns:
            List of terms
        """
        if prefix:
            terms = [term for term in self.hash_table.keys() 
                     if term.startswith(prefix)]
        else:
            terms = list(self.hash_table.keys())
        
        # Sort alphabetically
        terms.sort()
        
        # Apply limit
        return terms[:limit]
    
    def get_document_frequency(self, term: str) -> int:
        """
        Get the document frequency for a term.
        
        Args:
            term: Term to look up
            
        Returns:
            Number of documents containing the term, or 0 if not found
        """
        entry = self.hash_table.get(term)
        return entry.get("doc_count", 0) if entry else 0