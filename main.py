# search_engine/main.py
"""
Main entry point for the search engine application.
"""
import os
import sys
import argparse
import logging
import json
import time
from pathlib import Path

# Your project's specific imports
from src.parser.xml_parser import WSJParser, MalformedXMLError
from src.indexer.dictionary_builder import DictionaryBuilder
from src.search.retrieval import SearchEngine
from src.parser.models import Document 

# Set up logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout 
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Search Engine')

    subparsers = parser.add_subparsers(dest='command', help='Command to run', required=True)

    # Parser for 'parse' command
    parse_parser = subparsers.add_parser('parse', help='Parse WSJ XML files')
    parse_parser.add_argument(
        'input_file',
        help='Path to the WSJ XML file'
    )
    parse_parser.add_argument(
        '--output-dir',
        default='data',
        help='Directory to store parsed documents'
    )

    # Parser for 'index' command
    index_parser = subparsers.add_parser('index', help='Build index from parsed documents')
    index_parser.add_argument(
        'document_store',
        help='Path to the document store file (e.g., data/document_store.jsonl)'
    )
    index_parser.add_argument(
        '--output-dir',
        default='index',
        help='Directory to store index files'
    )

    # Parser for 'search' command
    search_parser = subparsers.add_parser('search', help='Search for documents (reads query from stdin)')
    search_parser.add_argument(
        '--index-dir',
        default='index',
        help='Directory containing index files'
    )
    search_parser.add_argument(
        '--document-store',
        default='data/document_store.jsonl',
        help='Path to the document store file'
    )
    search_parser.add_argument(
        '--max-results',
        type=int,
        default=None,  # Show all results unless specified
        help='Maximum number of results to return'
    )
    search_parser.add_argument(
        '--retrieve-documents',
        action='store_true',
        help='Whether to retrieve full documents (only for internal use/debugging)'
    )

    return parser.parse_args()


def parse_command(args):
    """Execute the parse command."""
    logger.info(f"Attempting to parse WSJ XML file: {args.input_file}")

    if not os.path.exists(args.input_file):
        logger.error(f"Input file not found: {args.input_file}")
        return {"num_documents": 0, "parse_time": 0, "document_store": None}

    try:
        os.makedirs(args.output_dir, exist_ok=True)
    except Exception as e:
        logger.error(f"Failed to create output directory {args.output_dir}: {e}")
        return {"num_documents": 0, "parse_time": 0, "document_store": None}

    parser = WSJParser()
    start_time = time.time()
    documents = []
    try:
        documents = parser.parse_file(args.input_file)
    except FileNotFoundError as e:
        logger.error(f"Error during parsing - File Not Found: {e}")
        return {"num_documents": 0, "parse_time": time.time() - start_time, "document_store": None}
    except MalformedXMLError as e:
         logger.error(f"Error during parsing - Malformed XML: {e}")
         return {"num_documents": 0, "parse_time": time.time() - start_time, "document_store": None}
    except Exception as e:
        logger.error(f"Unexpected error during parsing: {e}", exc_info=True)
        return {"num_documents": 0, "parse_time": time.time() - start_time, "document_store": None}

    parse_time = time.time() - start_time

    if not documents:
         logger.warning(f"No documents were parsed from {args.input_file}. Check file content and parser logic.")

    logger.info(f"Parsed {len(documents)} documents in {parse_time:.2f} seconds")

    document_store_path = os.path.join(args.output_dir, "document_store.jsonl")
    written_count = 0
    try:
        with open(document_store_path, "w", encoding="utf-8") as f:
            for doc in documents:
                doc_data = {
                    "docno": doc.docno,
                    "docid": doc.docid,
                    "headline": doc.headline,
                    "date": doc.date,
                    "source": doc.source,
                    "content": doc.content
                }
                if not doc.docno:
                     logger.warning(f"Document found with empty docno. Skipping write. Data: {doc_data}")
                     continue

                json.dump(doc_data, f)
                f.write("\n")
                written_count += 1
    except Exception as e:
        logger.error(f"Failed to write document store {document_store_path}: {e}", exc_info=True)
        return {"num_documents": len(documents), "parse_time": parse_time, "document_store": None}

    logger.info(f"Wrote {written_count} documents to document store: {document_store_path}")

    return {
        "num_documents": written_count, # Report documents actually written
        "parse_time": parse_time,
        "document_store": document_store_path
    }


def index_command(args):
    """Execute the index command."""
    logger.info(f"Building index from document store: {args.document_store}")

    if not os.path.exists(args.document_store):
        logger.error(f"Document store file not found: {args.document_store}")
        return None

    try:
        os.makedirs(args.output_dir, exist_ok=True)
    except Exception as e:
         logger.error(f"Failed to create output directory {args.output_dir}: {e}")
         return None

    documents_data = []
    logger.info(f"Loading documents from {args.document_store}...")
    try:
        with open(args.document_store, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f):
                try:
                    doc_data = json.loads(line.strip())
                    if not doc_data.get("docno"):
                         logger.warning(f"Skipping document at line {line_num+1} due to missing 'docno'.")
                         continue
                    documents_data.append(doc_data)
                except json.JSONDecodeError as json_e:
                    logger.error(f"Error decoding JSON at line {line_num+1} in {args.document_store}: {json_e}")
                    continue
    except Exception as e:
        logger.error(f"Failed to read document store {args.document_store}: {e}", exc_info=True)
        return None

    if not documents_data:
         logger.error(f"No valid documents loaded from {args.document_store}. Cannot build index.")
         return None

    documents = [
        Document(
            docno=doc.get("docno", ""),
            docid=doc.get("docid", ""),
            headline=doc.get("headline", ""),
            date=doc.get("date", ""),
            source=doc.get("source", ""),
            content=doc.get("content", "")
        )
        for doc in documents_data
    ]

    logger.info(f"Loaded {len(documents)} valid documents from document store")

    dictionary_builder = DictionaryBuilder(output_dir=args.output_dir)
    logger.info(f"Starting index build process into {args.output_dir}...")
    start_time = time.time()
    stats = None
    try:
        stats = dictionary_builder.build_index(documents)
    except Exception as e:
        logger.error(f"Error during index building: {e}", exc_info=True)
        return None
    index_time = time.time() - start_time

    if stats is None:
        logger.error("Index building failed to return statistics.")
        return None

    logger.info(f"Built index in {index_time:.2f} seconds")
    logger.info(f"Index statistics: {stats}")

    stats["index_time"] = index_time

    stats_path = os.path.join(args.output_dir, "index_stats.json")
    try:
        with open(stats_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2)
        logger.info(f"Wrote index statistics to: {stats_path}")
    except Exception as e:
        logger.error(f"Failed to write index statistics to {stats_path}: {e}")

    return stats


def search_command(args):
    """Execute the search command, reading the query from stdin."""
    logger.info("Waiting for query from stdin...")
    query_string = sys.stdin.readline().strip()
    if not query_string:
        logger.error("No query received from stdin.")
        return {"query": "", "num_results": 0, "search_time": 0}
    logger.info(f"Received query from stdin: '{query_string}'")

    stats_path = os.path.join(args.index_dir, "index_stats.json")
    total_documents = 0
    index_type = "hash"

    if os.path.exists(stats_path):
        try:
            with open(stats_path, "r", encoding="utf-8") as f:
                stats = json.load(f)
            total_documents = stats.get("num_documents", 0)
            index_type = stats.get("index_type", "hash")
            logger.info(f"Loaded stats: Found {index_type} index with {total_documents} documents.")
        except Exception as e:
            logger.error(f"Error loading index statistics from {stats_path}: {e}")
    else:
        logger.warning(f"Index statistics file not found: {stats_path}. Proceeding with defaults (0 documents).")

    required_files_exist = True
    postings_path = os.path.join(args.index_dir, "postings.bin")
    if not os.path.exists(postings_path):
        logger.error(f"Required index file not found: {postings_path}")
        required_files_exist = False

    if index_type == "hash":
        hash_table_path = os.path.join(args.index_dir, "hash_table.pkl")
        if not os.path.exists(hash_table_path):
            logger.error(f"Required index file not found: {hash_table_path}")
            required_files_exist = False
    # Add checks for other index types if implemented

    if not required_files_exist:
         logger.error("Essential index files are missing. Cannot perform search.")
         return {"query": query_string, "num_results": 0, "search_time": 0}

    search_engine = None
    result = None
    search_time = 0
    try:
        search_engine = SearchEngine(
            index_dir=args.index_dir,
            document_store_path=args.document_store,
            total_documents=total_documents
        )
    except Exception as e:
        logger.error(f"Failed to initialize SearchEngine: {e}", exc_info=True)
        return {"query": query_string, "num_results": 0, "search_time": 0}

    start_time = time.time()
    try:
        result = search_engine.search(
            query_string,
            max_results=args.max_results,
            retrieve_documents=args.retrieve_documents # Not used for assignment output but kept internally
        )
    except Exception as e:
        logger.error(f"An error occurred during search execution: {e}", exc_info=True)
        result = {"query": query_string, "num_results": 0, "results": []}
    finally:
        search_time = time.time() - start_time
        if search_engine and hasattr(search_engine, 'close'):
             search_engine.close()

    if result is None:
         logger.error("Search function returned None unexpectedly.")
         return {"query": query_string, "num_results": 0, "search_time": search_time}

    logger.info(f"Search completed in {search_time:.4f} seconds")
    logger.info(f"Found {result.get('total_results', result.get('num_results', 0))} total matches")

    results_list = result.get("results", [])
    for doc in results_list:
        if 'docno' not in doc or 'score' not in doc:
             logger.warning(f"Skipping malformed result: {doc}")
             continue
        try:
            score_str = f"{doc['score']:.4f}"
        except TypeError:
             score_str = "0.0000"
             logger.warning(f"Non-numeric score encountered for {doc['docno']}: {doc['score']}")

        # Standard output for the assignment - ONLY docno and score
        print(f"{doc['docno']} {score_str}")

    return {
        "query": query_string,
        "num_results": len(results_list),
        "search_time": search_time
    }


def main():
    """Main entry point."""
    args = parse_arguments()

    if args.command == 'parse':
        parse_command(args)
    elif args.command == 'index':
        index_command(args)
    elif args.command == 'search':
        search_command(args)

    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except Exception as e:
         logger.error(f"Unhandled exception in main execution: {e}", exc_info=True)
         sys.exit(1)