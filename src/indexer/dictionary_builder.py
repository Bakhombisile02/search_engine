"""
Dictionary builder for the indexer component.
"""
import os
import logging
from collections import Counter
from typing import List, Dict, Any, Tuple

# Ensure relative import works correctly
try:
    from ..parser.models import Document
    from ..parser.text_normalizer import TextNormalizer
    from ..indexer.hash_index import HashIndex
except ImportError:
    # Fallback for direct execution or different environment setups
    from src.parser.models import Document
    from src.parser.text_normalizer import TextNormalizer
    from src.indexer.hash_index import HashIndex


logger = logging.getLogger(__name__)


class DictionaryBuilder:
    """Builds a dictionary and postings lists from parsed documents."""

    def __init__(
        self,
        output_dir: str = "index",
        normalizer: TextNormalizer = None
    ):
        """
        Initialize the dictionary builder.

        Args:
            output_dir: Directory to store index files
            normalizer: Text normalizer for term processing
        """
        self.output_dir = output_dir
        self.normalizer = normalizer or TextNormalizer()
        self.hash_index = HashIndex(output_dir)

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Path for stats file
        self.stats_path = os.path.join(output_dir, "index_stats.json")

    def build_index(self, documents: List[Document]) -> Dict[str, Any]:
        """
        Build an index from a list of documents.

        Args:
            documents: List of parsed documents

        Returns:
            Dictionary with indexing statistics
        """
        logger.info(f"Building index from {len(documents)} documents")

        # Step 1: Generate term-document pairs (modified to include all fields)
        term_doc_pairs = self._generate_term_doc_pairs(documents)
        logger.info(f"Generated {len(term_doc_pairs)} term-document pairs")

        # Step 2: Sort pairs by term (inversion process)
        term_doc_pairs.sort(key=lambda x: x[0])
        logger.info("Sorted term-document pairs")

        # Step 3: Build hash-based index
        index_stats = self.hash_index.build_index(term_doc_pairs)
        logger.info(f"Built hash index with {index_stats['num_terms']} terms")

        # Combine statistics
        stats = {
            "num_documents": len(documents),
            "index_type": "hash",
            **index_stats
        }

        # Return statistics
        return stats

    def _generate_term_doc_pairs(self, documents: List[Document]) -> List[Tuple[str, str, int]]:
        """
        Generate term-document pairs from documents, including terms from all
        specified fields (docno, docid, headline, date, source, content).

        Args:
            documents: List of parsed documents

        Returns:
            List of tuples (term, docno, frequency)
        """
        term_doc_pairs = []

        for document in documents:
            # Combine text from all searchable fields
            # Ensure all fields are strings before joining
            combined_text = " ".join(filter(None, [
                document.docno,
                document.docid,
                document.headline,
                document.date,
                document.source,
                document.content
            ]))

            # Normalize and tokenize combined document text
            normalized_text = self.normalizer.normalize_text(combined_text)
            tokens = self.normalizer.tokenize(normalized_text)

            # Count term frequencies within the combined text for this document
            term_counts = Counter(tokens)

            # Add pairs (term, docno, frequency)
            for term, frequency in term_counts.items():
                # Skip empty terms if any slip through (though tokenizer should prevent this)
                if term:
                    term_doc_pairs.append((term, document.docno, frequency))


        return term_doc_pairs