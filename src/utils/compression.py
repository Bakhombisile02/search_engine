"""
Compression utilities for efficient storage of index structures.
"""
import struct
from typing import List, Dict, Any


class Compression:
    """Class for compression and decompression of index data."""
    
    @staticmethod
    def zigzag_encode(number: int) -> int:
        """
        Encode a signed integer using ZigZag encoding.
        ZigZag encoding maps signed integers to unsigned integers in a zigzag pattern:
        0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, etc.
        
        Args:
            number: Signed integer to encode
            
        Returns:
            Unsigned integer in ZigZag encoding
        """
        # (n << 1) ^ (n >> 31) for 32-bit integers
        # (n << 1) ^ (n >> 63) for 64-bit integers
        # We'll use a simpler version that works for Python integers
        return (number << 1) ^ (number >> 63) if number < 0 else (number << 1)
    
    @staticmethod
    def zigzag_decode(number: int) -> int:
        """
        Decode a ZigZag encoded unsigned integer back to a signed integer.
        
        Args:
            number: Unsigned integer in ZigZag encoding
            
        Returns:
            Original signed integer
        """
        return (number >> 1) ^ (-(number & 1))
    
    @staticmethod
    def encode_vbyte(number: int) -> bytes:
        """
        Encode an integer using ZigZag and variable byte encoding.
        Supports both positive and negative integers.
        
        Args:
            number: Integer to encode (can be negative)
            
        Returns:
            Variable byte encoded representation as bytes
        """
        # First, convert to unsigned using ZigZag encoding
        unsigned = Compression.zigzag_encode(number)
        
        if unsigned == 0:
            return bytes([0])
        
        result = bytearray()
        
        while unsigned > 0:
            # Get 7 bits and set the continuation bit (MSB)
            byte = unsigned & 0x7F
            unsigned >>= 7
            
            # Set the continuation bit if there are more bytes
            if unsigned > 0:
                byte |= 0x80
            
            result.append(byte)
        
        return bytes(result)
    
    @staticmethod
    def decode_vbyte(data: bytes, start_pos: int = 0) -> tuple[int, int]:
        """
        Decode a variable byte encoded integer, applying ZigZag decoding.
        
        Args:
            data: Bytes containing the encoded data
            start_pos: Starting position in the data
            
        Returns:
            Tuple containing (decoded signed number, new position)
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
        
        # Apply ZigZag decoding to get back the signed integer
        signed_result = Compression.zigzag_decode(result)
        
        return signed_result, pos
    
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
        
        # Store a flag for WSJ document format (hyphen after prefix + 6 digits)
        # Used to indicate this is WSJ format: WSJ870108-0012
        # where hyphen occurs after prefix (WSJ) + 6 digits
        hyphen_flag = 1
        result.extend(Compression.encode_vbyte(hyphen_flag))
        
        # Encode number of postings
        result.extend(Compression.encode_vbyte(len(postings)))
        
        # Analyze the first docno to extract the prefix
        if postings:
            sample_docno = postings[0]['docno']
            
            # Extract the prefix (usually "WSJ")
            prefix = ""
            for char in sample_docno:
                if char.isalpha():
                    prefix += char
                else:
                    break
                    
            # Store prefix length
            result.extend(Compression.encode_vbyte(len(prefix)))
            
            # Store prefix characters
            for char in prefix:
                result.extend(Compression.encode_vbyte(ord(char)))
        else:
            # No postings, just store empty prefix
            result.extend(Compression.encode_vbyte(0))
        
        prev_docno = 0
        for posting in postings:
            # Extract just the numeric part of the docno
            docno_str = posting['docno']
            
            # Remove prefix and hyphen, keeping only digits
            numeric_part = ''.join(c for c in docno_str if c.isdigit())
            
            if numeric_part:
                numeric_docno = int(numeric_part)
            else:
                numeric_docno = 0  # Fallback if no numeric part
            
            # Calculate delta - can be negative in some cases
            delta = numeric_docno - prev_docno
            
            # Encode delta (using ZigZag for negative numbers) and frequency
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
        
        # Read hyphen position (not used now, but kept for backward compatibility)
        hyphen_pos, pos = Compression.decode_vbyte(data, pos)
        
        # Decode number of postings
        num_postings, pos = Compression.decode_vbyte(data, pos)
        
        # Read prefix length
        prefix_len, pos = Compression.decode_vbyte(data, pos)
        
        # Read prefix characters
        prefix = ""
        for _ in range(prefix_len):
            char_code, pos = Compression.decode_vbyte(data, pos)
            prefix += chr(char_code)
        
        prev_docno = 0
        for _ in range(num_postings):
            # Decode delta - could be negative after ZigZag decoding
            delta, pos = Compression.decode_vbyte(data, pos)
            
            # Decode frequency
            frequency, pos = Compression.decode_vbyte(data, pos)
            
            # Calculate actual docno
            numeric_docno = prev_docno + delta
            
            # Format docno with hyphen at the correct position for WSJ format
            numeric_str = str(numeric_docno)
            
            # For standard WSJ format (WSJ870108-0012):
            # - Prefix is "WSJ"
            # - Followed by 6 digits for date (YYMMDD)
            # - Followed by hyphen
            # - Followed by 4 digits for document number
            
            # Make sure numeric part has enough digits
            numeric_str = numeric_str.zfill(10)  # Total of 10 digits (6+4)
            
            # Place hyphen after the 6th digit
            docno = f"{prefix}{numeric_str[:6]}-{numeric_str[6:]}"
            
            postings.append({
                'docno': docno,
                'frequency': frequency
            })
            
            prev_docno = numeric_docno
        
        return postings