
# WSJ Search Engine

## Overview

This project implements a full-text search engine for the Wall Street Journal (WSJ) corpus, developed for the COSC431 Information Retrieval course at the University of Otago. It features robust XML parsing capable of handling potentially malformed files, text normalization, efficient hash-based indexing with compression, and relevance-ranked query processing using TF-IDF. The system is designed for modularity, separating concerns between parsing, indexing, and search functionalities, and aims to meet sub-second query performance requirements.

## Features

The search engine incorporates several key features:

- **XML Parsing**: Processes WSJ XML documents, extracting metadata and content. It is specifically designed to handle real-world, potentially non-well-formed XML data gracefully.
- **Text Normalization**: Standardizes text by converting it to lowercase, removing punctuation, replacing common HTML entities, handling hyphens consistently, and tokenizing the content into individual terms (derived from `search_engine/src/parser/text_normalizer.py`). Note that the current implementation does not perform stemming or stopword removal.
- **Hash-Based Indexing**: Builds an inverted index using a hash table structure. This allows for fast term lookups, typically achieving O(1) average time complexity (derived from `search_engine/src/indexer/hash_index.py`).
- **Efficient Storage**: Employs several compression techniques, including VByte, ZigZag encoding, delta encoding for document numbers, and prefix compression. These methods significantly reduce the storage space required for the index files (derived from `search_engine/src/utils/compression.py`).
- **Relevance Ranking**: Uses the TF-IDF (Term Frequency-Inverse Document Frequency) weighting scheme to rank documents. It applies logarithmic normalization to the term frequency component for scoring (derived from `search_engine/src/search/hash_query_processor.py`).
- **Command-Line Interface**: Provides straightforward tools to parse the source XML corpus, build the search index, and perform search queries (derived from `search_engine/main.py`).
- **Fast Query Processing**: Optimized to deliver search results quickly, aiming for sub-second performance per query as required. It reads queries from standard input and writes the ranked results to standard output.

## System Architecture

The search engine follows a modular design, structured into four main components:

1. **Parser Module (`src/parser/`)**: Handles the initial processing of the WSJ XML corpus. Includes `xml_parser.py` for parsing, `text_normalizer.py` for cleaning and standardizing the text, and `models.py` for defining the `Document` data structure.
2. **Indexer Module (`src/indexer/`)**: Builds the core search index. Includes `hash_index.py` (hash-based inverted index structure), `dictionary_builder.py` (creates term dictionary and postings lists), and uses Python's `pickle` and custom binary formats.
3. **Search Module (`src/search/`)**: Handles user queries. Includes `hash_query_processor.py` (query input, lookup, scoring), and `retrieval.py` (search orchestration).
4. **Utils Module (`src/utils/`)**: Shared utilities. Includes `compression.py` (compression methods) and `file_io.py` (file read/write functions).

### Data Flow

1. Raw WSJ XML corpus (`data/wsj.xml`) is fed into the **Parser Module**.
2. Parser outputs normalized documents (`data/document_store.jsonl`).
3. **Indexer Module** creates the inverted index files in `index/`.
4. **Search Module** uses the index to process user queries from standard input and outputs ranked document results.

## Installation

### Prerequisites

- Python 3.8 or newer.
- Python libraries in `requirements.txt` (`numpy`, `pytest`, `mmap3`, `cython`).

### Setup

```bash
# Clone the repository
# git clone <repository-url>

cd search_engine

# Install dependencies
pip install -r requirements.txt

# (Optional) Install the package
# python setup.py install


## Usage

### Step 1: Add the WSJ XML Corpus
Add the WSJ XML file 'wsj.xml' to the `data/` directory. 

### Step 2: Parse the Corpus
```bash
python main.py parse data/wsj.xml --output-dir data
```

### Step 3: Build the Index
```bash
python main.py index data/document_store.jsonl --output-dir index
```

### Step 4: Perform a Search
```bash
# Single-term search
echo "Daminozide" | python main.py search --index-dir index --document-store data/document_store.jsonl

# Multi-term search
echo "economic policy" | python main.py search --index-dir index --document-store data/document_store.jsonl
```

### Full Pipeline
```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

### Search Output Format

```
<docno1> <rsv1>
<docno2> <rsv2>
...
```

Example:
```
WSJ870108-0012 8.2446
WSJ880421-0049 6.8488
WSJ891003-0193 4.6366
WSJ890905-0094 4.6366
```

## Index Structure

- `hash_table.pkl`: Hash table mapping terms to postings metadata.
- `postings.bin`: Binary file with compressed postings lists.
- `index_stats.json`: Metadata on indexing (doc count, term count, time, etc.).

## Performance

The system delivers O(1) term lookup and uses compression techniques (VByte, ZigZag, delta encoding) to reduce disk usage. Designed for sub-second query processing.

## File Structure

```
search_engine/
├── main.py
├── README.md
├── requirements.txt
├── run_pipeline.sh
├── setup.py
├── data/
│   ├── document_store.jsonl
│   └── wsj.xml
├── index/
│   ├── hash_table.pkl
│   ├── index_stats.json
│   └── postings.bin
└── src/
    ├── indexer/
    │   ├── dictionary_builder.py
    │   └── hash_index.py
    ├── parser/
    │   ├── models.py
    │   ├── text_normalizer.py
    │   └── xml_parser.py
    ├── search/
    │   ├── hash_query_processor.py
    │   ├── query_processor.py
    │   └── retrieval.py
    └── utils/
        ├── compression.py
        └── file_io.py
```

## Contributing

1. Fork the repository
2. Create a branch: `git checkout -b feature/YourFeature`
3. Commit your changes: `git commit -m 'Add YourFeature'`
4. Push: `git push origin feature/YourFeature`
5. Open a Pull Request

## License

Distributed under the MIT License. See `LICENSE` for more information.

## Acknowledgments

- Developed by **Bakhombisile Siyamukela Dlamini** for the **COSC431 Information Retrieval** course at the **University of Otago**, New Zealand
- Uses the WSJ corpus provided for academic purposes.
```

