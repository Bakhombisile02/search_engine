"""
Compression utilities for efficient storage of index structures.
"""
import struct
from typing import List, Dict, Any


class Compression:
    """Class for compression and decompression of index data."""
    
    @staticmethod
    def encode_vbyte(number: int) -> bytes:
        """
        Encode an integer using variable byte encoding.
        
        Args:
            number: Integer to encode
            
        Returns:
            Variable byte encoded representation as bytes
        """
        if number < 0:
            raise ValueError("Variable byte encoding only supports non-negative integers")
        
        if number == 0:
            return bytes([0])
        
        result = bytearray()
        
        while number > 0:
            # Get 7 bits and set the continuation bit (MSB)
            byte = number & 0x7F
            number >>= 7
            
            # Set the continuation bit if there are more bytes
            if number > 0:
                byte |= 0x80
            
            result.append(byte)
        
        return bytes(result)
    
    @staticmethod
    def decode_vbyte(data: bytes, start_pos: int = 0) -> tuple[int, int]:
        """
        Decode a variable byte encoded integer.
        
        Args:
            data: Bytes containing the encoded data
            start_pos: Starting position in the data
            
        Returns:
            Tuple containing (decoded number, new position)
        """
        result = 0
        shift = 0
        pos = start_pos
        
        while pos < len(data):
            byte = data[pos]
            pos += 1
            
            # Add the 7 bits to our result
            result |= (byte & 0x7F) << shift
            shift += 7
            
            # If the continuation bit is not set, we're done
            if not (byte & 0x80):
                break
        
        return result, pos
    
    @staticmethod
    def compress_postings(postings: List[Dict[str, Any]]) -> bytes:
        """
        Compress a postings list using delta encoding and variable byte encoding.
        
        Args:
            postings: List of postings, each with 'docno' and 'frequency' keys
            
        Returns:
            Compressed postings as bytes
        """
        result = bytearray()
        
        # Encode number of postings
        result.extend(Compression.encode_vbyte(len(postings)))
        
        prev_docno = 0
        for posting in postings:
            # Extract numeric part of docno for delta encoding
            # Assuming docno format like "WSJ920102-0154", extract "9201020154"
            docno_str = posting['docno']
            numeric_docno = int(''.join(c for c in docno_str if c.isdigit()))
            
            # Calculate delta
            delta = numeric_docno - prev_docno
            
            # Encode delta and frequency
            result.extend(Compression.encode_vbyte(delta))
            result.extend(Compression.encode_vbyte(posting['frequency']))
            
            prev_docno = numeric_docno
        
        return bytes(result)
    
    @staticmethod
    def decompress_postings(data: bytes) -> List[Dict[str, Any]]:
        """
        Decompress postings from bytes to a list of postings.
        
        Args:
            data: Compressed postings bytes
            
        Returns:
            List of postings, each with 'docno' and 'frequency' keys
        """
        postings = []
        pos = 0
        
        # Decode number of postings
        num_postings, pos = Compression.decode_vbyte(data, pos)
        
        prev_docno = 0
        for _ in range(num_postings):
            # Decode delta
            delta, pos = Compression.decode_vbyte(data, pos)
            
            # Decode frequency
            frequency, pos = Compression.decode_vbyte(data, pos)
            
            # Calculate actual docno
            numeric_docno = prev_docno + delta
            
            # Convert numeric docno back to string format (simplified)
            # This is a basic conversion - you may need to adjust based on actual format
            docno = f"WSJ{numeric_docno}"
            
            postings.append({
                'docno': docno,
                'frequency': frequency
            })
            
            prev_docno = numeric_docno
        
        return postings