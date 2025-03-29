"""
Text normalization utilities for the parser component.
"""
import re
import html
from typing import List


class TextNormalizer:
    """Class for text normalization operations."""

    # Regular expressions for cleaning text
    PUNCTUATION_PATTERN = re.compile(r'[^\w\s-]')
    WHITESPACE_PATTERN = re.compile(r'\s+')
    HTML_ENTITY_PATTERN = re.compile(r'&[a-zA-Z]+;')

    # Common HTML entities
    HTML_ENTITIES = {
        '&amp;': '&',
        '&lt;': '<',
        '&gt;': '>',
        '&quot;': '"',
        '&apos;': "'",
    }

    # Common stopwords that could be filtered out (REMOVED for barebones version)
    STOPWORDS = set([
        
    ])

    @classmethod
    def normalize_text(cls, text: str) -> str:
        """
        Normalize text by converting to lowercase, removing punctuation,
        and handling special cases.

        Args:
            text: The input text to normalize

        Returns:
            Normalized text
        """
        if not text:
            return ""

        # Replace HTML entities
        text = cls.replace_html_entities(text)

        # Convert to lowercase
        text = text.lower()

        # Remove punctuation and special characters
        text = cls.remove_punctuation(text)

        # Handle hyphenated words (contractions removed)
        text = cls.handle_special_cases(text)

        # Remove extra whitespace
        text = cls.WHITESPACE_PATTERN.sub(' ', text).strip()

        return text

    @classmethod
    def replace_html_entities(cls, text: str) -> str:
        """
        Replace HTML entities with their corresponding characters.

        Args:
            text: The input text containing HTML entities

        Returns:
            Text with HTML entities replaced
        """
        # Use html module to unescape HTML entities
        return html.unescape(text)

    @classmethod
    def remove_punctuation(cls, text: str) -> str:
        """
        Remove punctuation and special characters from text.

        Args:
            text: The input text

        Returns:
            Text with punctuation removed
        """
        return cls.PUNCTUATION_PATTERN.sub(' ', text)

    @classmethod
    def handle_special_cases(cls, text: str) -> str:
        """
        Handle special cases like hyphenated words.
        Contraction handling has been removed.

        Args:
            text: The input text

        Returns:
            Text with special cases handled
        """
        # Replace hyphens with spaces for hyphenated words
        text = text.replace('-', ' ')

        return text

    @classmethod
    def tokenize(cls, text: str) -> List[str]:
        """
        Tokenize normalized text into individual words.

        Args:
            text: The normalized text to tokenize

        Returns:
            List of tokens
        """
        if not text:
            return []

        # Convert to lowercase again just to be sure
        text = text.lower()

        # Split on whitespace
        tokens = cls.WHITESPACE_PATTERN.split(text.strip())

        # Filter out empty tokens (stopwords filtering is implicitly removed as STOPWORDS set is empty)
        tokens = [token for token in tokens if token] # Removed check: 'and token not in cls.STOPWORDS'

        return tokens