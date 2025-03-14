"""
Document models for the parser component.
"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Document:
    """Represents a document from the WSJ corpus."""
    
    docno: str = ""  # Document number
    docid: str = ""  # Document ID
    date: str = ""  # Publication date
    headline: str = ""  # Headline
    source: str = ""  # Source
    content: str = ""  # Document content
    
    def __post_init__(self):
        """Initialize any empty fields."""
        if not self.content:
            self.content = ""