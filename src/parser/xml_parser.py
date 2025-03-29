"""
XML parser for processing WSJ corpus documents.
"""
import re
import logging
from typing import List, Optional, Iterator, TextIO
from pathlib import Path

from .models import Document
from .text_normalizer import TextNormalizer


logger = logging.getLogger(__name__)


class MalformedXMLError(Exception):
    """Exception raised when XML is malformed."""
    pass


class WSJParser:
    """Parser for WSJ XML corpus documents."""
    
    # Regular expressions for parsing
    DOC_START_PATTERN = re.compile(r'<DOC>')
    DOC_END_PATTERN = re.compile(r'</DOC>')
    ELEMENT_START_PATTERN = re.compile(r'<([^>]+)>')
    ELEMENT_END_PATTERN = re.compile(r'</([^>]+)>')
    CONTENT_PATTERN = re.compile(r'>([^<]*)</')
    
    def __init__(self, normalizer: Optional[TextNormalizer] = None):
        """
        Initialize the parser.
        
        Args:
            normalizer: Optional text normalizer instance
        """
        self.normalizer = normalizer or TextNormalizer()
    
    def parse_file(self, file_path: str) -> List[Document]:
        """
        Parse a WSJ XML file and return a list of documents.
        
        Args:
            file_path: Path to the XML file
            
        Returns:
            List of parsed Document objects
            
        Raises:
            FileNotFoundError: If the file is not found
            MalformedXMLError: If the XML is malformed
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        documents = []
        
        with open(file_path, 'r', encoding='utf-8') as file:
            for document in self.parse_stream(file):
                documents.append(document)
        
        return documents
    
    def parse_stream(self, file_stream: TextIO) -> Iterator[Document]:
        """
        Parse a stream of WSJ XML data and yield documents.
        
        Args:
            file_stream: An open file or file-like object
            
        Yields:
            Document objects parsed from the stream
            
        Raises:
            MalformedXMLError: If the XML is malformed
        """
        current_document = None
        current_element = None
        buffer = ""
        line_num = 0
        
        try:
            for line in file_stream:
                line_num += 1
                line = line.strip()
                
                # Handle empty lines
                if not line:
                    continue
                
                # Handle document start
                if self.DOC_START_PATTERN.search(line):
                    current_document = Document()
                    continue
                
                # Handle document end
                if self.DOC_END_PATTERN.search(line):
                    if current_document is not None:
                        # Format document number before yielding
                        if current_document.docno:
                            current_document.docno = self._format_docno(current_document.docno)
                        yield current_document
                        current_document = None
                        current_element = None
                    else:
                        logger.warning(f"Line {line_num}: Found </DOC> without matching <DOC>")
                    continue
                
                # Skip processing if not within a document
                if current_document is None:
                    continue
                
                # Handle element start
                element_start_match = self.ELEMENT_START_PATTERN.search(line)
                if element_start_match and not element_start_match.group(0).startswith('</'):
                    current_element = element_start_match.group(1)
                    
                    # Extract content if present on the same line
                    content_match = self.CONTENT_PATTERN.search(line)
                    if content_match:
                        content = content_match.group(1).strip()
                        self._update_document(current_document, current_element, content)
                        current_element = None
                    continue
                
                # Handle element end
                element_end_match = self.ELEMENT_END_PATTERN.search(line)
                if element_end_match:
                    if buffer and current_element:
                        self._update_document(current_document, current_element, buffer)
                    buffer = ""
                    current_element = None
                    continue
                
                # Handle content
                if current_element and current_document:
                    buffer += line + " "
        
        except Exception as e:
            logger.error(f"Error parsing at line {line_num}: {str(e)}")
            raise MalformedXMLError(f"Error parsing at line {line_num}: {str(e)}") from e
    
    def _update_document(self, document: Document, element: str, content: str) -> None:
        """
        Update a document with normalized content based on the element type.
        
        Args:
            document: The document to update
            element: The element name
            content: The content to add
        """
        content = content.strip()
        
        if element == "DOCNO":
            document.docno = content
        elif element == "DOCID":
            document.docid = content
        elif element == "HL":
            document.headline = content
        elif element == "DATE":
            document.date = content
        elif element == "SO":
            document.source = content
        elif element in ["LP", "TEXT", "P"]:
            normalized_content = self.normalizer.normalize_text(content)
            if document.content:
                document.content += " " + normalized_content
            else:
                document.content = normalized_content

    def _format_docno(self, doc_id: str) -> str:
        """
        Format document number to follow the pattern WSJ870108-0021.
        
        Args:
            doc_id: Original document ID
            
        Returns:
            Formatted document number
        """
        # Remove any non-alphanumeric characters
        clean_id = ''.join(c for c in doc_id if c.isalnum())
        
        # Format should be WSJyymmdd-nnnn where:
        # yy = year, mm = month, dd = day, nnnn = sequence number
        if len(clean_id) >= 10 and clean_id.startswith('WSJ'):
            wsj_part = clean_id[:3]
            date_part = clean_id[3:9]  # yymmdd
            seq_part = clean_id[9:]    # sequence number
            
            # Format to WSJyymmdd-nnnn
            return f"{wsj_part}{date_part}-{seq_part.zfill(4)}"
        
        # If it doesn't match expected format, return as is
        return doc_id