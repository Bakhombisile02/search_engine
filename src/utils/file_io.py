"""
File I/O utilities for reading and writing index structures.
"""
import os
import mmap
import struct
import pickle
import json
from pathlib import Path
from typing import List, Dict, Any, Optional, BinaryIO, TypeVar, Generic, Union
from contextlib import contextmanager

T = TypeVar('T')

class FileIO:
    """Class for file I/O operations."""
    
    @staticmethod
    def ensure_directory(directory: str) -> None:
        """
        Ensure that a directory exists, creating it if necessary.
        
        Args:
            directory: Directory path
        """
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def write_binary(data: bytes, file_path: str) -> None:
        """
        Write binary data to a file.
        
        Args:
            data: Binary data to write
            file_path: Path to the output file
        """
        # Ensure directory exists
        FileIO.ensure_directory(os.path.dirname(file_path))
        
        with open(file_path, 'wb') as file:
            file.write(data)
    
    @staticmethod
    def read_binary(file_path: str) -> bytes:
        """
        Read binary data from a file.
        
        Args:
            file_path: Path to the input file
            
        Returns:
            Binary data read from the file
            
        Raises:
            FileNotFoundError: If the file is not found
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        with open(file_path, 'rb') as file:
            return file.read()
    
    @staticmethod
    def write_pickle(data: Any, file_path: str) -> None:
        """
        Write data to a file using pickle serialization.
        
        Args:
            data: Data to write
            file_path: Path to the output file
        """
        # Ensure directory exists
        FileIO.ensure_directory(os.path.dirname(file_path))
        
        with open(file_path, 'wb') as file:
            pickle.dump(data, file)
    
    @staticmethod
    def read_pickle(file_path: str) -> Any:
        """
        Read data from a file using pickle deserialization.
        
        Args:
            file_path: Path to the input file
            
        Returns:
            Deserialized data
            
        Raises:
            FileNotFoundError: If the file is not found
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        with open(file_path, 'rb') as file:
            return pickle.load(file)
    
    @staticmethod
    @contextmanager
    def open_file(file_path: str, mode: str = 'rb'):
        """
        Context manager for safely opening and closing files.
        
        Args:
            file_path: Path to the file
            mode: File open mode
            
        Yields:
            Open file object
            
        Raises:
            FileNotFoundError: If the file is not found (for read modes)
        """
        if 'r' in mode and not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        if 'w' in mode:
            FileIO.ensure_directory(os.path.dirname(file_path))
            
        file = open(file_path, mode)
        try:
            yield file
        finally:
            file.close()
    
    @staticmethod
    @contextmanager
    def get_memory_mapped_file(file_path: str, readonly: bool = True):
        """
        Context manager for safely opening and closing memory-mapped files.
        
        Args:
            file_path: Path to the file
            readonly: Whether to open in read-only mode
            
        Yields:
            Memory-mapped file object
            
        Raises:
            FileNotFoundError: If the file is not found
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Open the file
        file = open(file_path, 'rb')
        
        try:
            # Memory map the file
            access = mmap.ACCESS_READ if readonly else mmap.ACCESS_WRITE
            mapped_file = mmap.mmap(file.fileno(), 0, access=access)
            try:
                yield mapped_file
            finally:
                mapped_file.close()
        finally:
            file.close()
    
    @staticmethod
    def write_json(data: Dict[str, Any], file_path: str, pretty: bool = True) -> None:
        """
        Write data to a JSON file.
        
        Args:
            data: Data to write
            file_path: Path to the output file
            pretty: Whether to format the JSON with indentation
        """
        # Ensure directory exists
        FileIO.ensure_directory(os.path.dirname(file_path))
        
        with open(file_path, 'w', encoding='utf-8') as file:
            if pretty:
                json.dump(data, file, indent=2)
            else:
                json.dump(data, file)
    
    @staticmethod
    def read_json(file_path: str) -> Dict[str, Any]:
        """
        Read data from a JSON file.
        
        Args:
            file_path: Path to the input file
            
        Returns:
            Deserialized JSON data
            
        Raises:
            FileNotFoundError: If the file is not found
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    
    @staticmethod
    def write_hash_index(hash_table: Dict[str, Any], file_path: str) -> None:
        """
        Write a hash table optimized for index lookups.
        
        Args:
            hash_table: Hash table to write
            file_path: Path to the output file
        """
        # Sort terms for better compression
        sorted_items = sorted(hash_table.items())
        
        # Ensure directory exists
        FileIO.ensure_directory(os.path.dirname(file_path))
        
        with open(file_path, 'wb') as file:
            # Write number of entries
            file.write(struct.pack('!I', len(sorted_items)))
            
            # Write each entry
            for term, entry in sorted_items:
                # Write term
                term_bytes = term.encode('utf-8')
                file.write(struct.pack('!H', len(term_bytes)))
                file.write(term_bytes)
                
                # Write location, length, and doc_count
                file.write(struct.pack('!Q', entry['location']))
                file.write(struct.pack('!I', entry['length']))
                file.write(struct.pack('!I', entry.get('doc_count', 0)))
    
    @staticmethod
    def read_hash_index(file_path: str) -> Dict[str, Dict[str, Any]]:
        """
        Read a hash table optimized for index lookups.
        
        Args:
            file_path: Path to the input file
            
        Returns:
            Hash table with term entries
            
        Raises:
            FileNotFoundError: If the file is not found
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        hash_table = {}
        
        with open(file_path, 'rb') as file:
            # Read number of entries
            num_entries = struct.unpack('!I', file.read(4))[0]
            
            # Read each entry
            for _ in range(num_entries):
                # Read term
                term_len = struct.unpack('!H', file.read(2))[0]
                term = file.read(term_len).decode('utf-8')
                
                # Read location, length, and doc_count
                location = struct.unpack('!Q', file.read(8))[0]
                length = struct.unpack('!I', file.read(4))[0]
                doc_count = struct.unpack('!I', file.read(4))[0]
                
                hash_table[term] = {
                    'location': location,
                    'length': length,
                    'doc_count': doc_count
                }
        
        return hash_table
    
    @staticmethod
    def write_root_to_file(root_terms: List[Dict[str, Any]], file_path: str) -> None:
        """
        Write root terms to a binary file for ISAM index.
        
        Args:
            root_terms: List of root term entries
            file_path: Path to the output file
        """
        # Ensure directory exists
        FileIO.ensure_directory(os.path.dirname(file_path))
        
        with open(file_path, 'wb') as file:
            # Write number of entries
            file.write(struct.pack('!I', len(root_terms)))
            
            # Write each entry
            for entry in root_terms:
                # Write term
                term_bytes = entry['term'].encode('utf-8')
                file.write(struct.pack('!H', len(term_bytes)))
                file.write(term_bytes)
                
                # Write location
                file.write(struct.pack('!Q', entry['location']))
    
    @staticmethod
    def read_root_from_file(file_path: str) -> List[Dict[str, Any]]:
        """
        Read root terms from a binary file for ISAM index.
        
        Args:
            file_path: Path to the input file
            
        Returns:
            List of root term entries
            
        Raises:
            FileNotFoundError: If the file is not found
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        root_terms = []
        
        with open(file_path, 'rb') as file:
            # Read number of entries
            num_entries = struct.unpack('!I', file.read(4))[0]
            
            # Read each entry
            for _ in range(num_entries):
                # Read term
                term_len = struct.unpack('!H', file.read(2))[0]
                term = file.read(term_len).decode('utf-8')
                
                # Read location
                location = struct.unpack('!Q', file.read(8))[0]
                
                root_terms.append({
                    'term': term,
                    'location': location
                })
        
        return root_terms
    
    @staticmethod
    def write_leaves_to_file(
        dictionary: List[Dict[str, Any]], 
        root_terms: List[Dict[str, Any]], 
        file_path: str
    ) -> None:
        """
        Write leaf nodes to a binary file for ISAM index.
        
        Args:
            dictionary: Complete dictionary
            root_terms: Root term entries
            file_path: Path to the output file
        """
        # Ensure directory exists
        FileIO.ensure_directory(os.path.dirname(file_path))
        
        with open(file_path, 'wb') as file:
            # Determine leaf blocks
            if not root_terms:
                return
            
            # Get terms from root
            root_term_values = [entry['term'] for entry in root_terms]
            
            # Sort root terms
            root_term_values.sort()
            
            # Add sentinel value
            root_term_values.append("ZZZZZZZZZZZ")  # Sentinel value
            
            # Write each leaf block
            start_idx = 0
            for i in range(len(root_term_values) - 1):
                current_term = root_term_values[i]
                next_term = root_term_values[i + 1]
                
                # Find entries in this range
                block_entries = []
                for entry in dictionary:
                    if current_term <= entry['term'] < next_term:
                        block_entries.append(entry)
                
                # Write block size
                file.write(struct.pack('!I', len(block_entries)))
                
                # Write each entry in the block
                for entry in block_entries:
                    # Write term
                    term_bytes = entry['term'].encode('utf-8')
                    file.write(struct.pack('!H', len(term_bytes)))
                    file.write(term_bytes)
                    
                    # Write length and location
                    file.write(struct.pack('!I', entry['length']))
                    file.write(struct.pack('!Q', entry['location']))
    
    @staticmethod
    def read_leaves_block(file: BinaryIO, block_location: int) -> List[Dict[str, Any]]:
        """
        Read a leaf block from an ISAM index file.
        
        Args:
            file: Open binary file
            block_location: Location of the block in the file
            
        Returns:
            List of dictionary entries in the block
        """
        # Seek to block location
        file.seek(block_location)
        
        # Read block size
        block_size = struct.unpack('!I', file.read(4))[0]
        
        # Read entries
        entries = []
        for _ in range(block_size):
            # Read term
            term_len = struct.unpack('!H', file.read(2))[0]
            term = file.read(term_len).decode('utf-8')
            
            # Read length and location
            length = struct.unpack('!I', file.read(4))[0]
            location = struct.unpack('!Q', file.read(8))[0]
            
            entries.append({
                'term': term,
                'length': length,
                'location': location
            })
        
        return entries