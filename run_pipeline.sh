#!/bin/bash

# Clean up previous runs
echo "Removing existing index and document store..."
rm -rf index/*
rm -f data/document_store.jsonl
rm -f results.txt # Remove previous results file

# Ensure directories exist
mkdir -p data
mkdir -p index

# Parse the WSJ XML file
echo "Parsing WSJ XML file (data/wsj.xml)..."
python main.py parse data/wsj.xml --output-dir data

# Build the index from the parsed documents
echo "Building index (from data/document_store.jsonl)..."
python main.py index data/document_store.jsonl --output-dir index

# Perform search for "Daminozide", piping query via stdin and redirecting results
echo "Performing search for 'Daminozide' and outputting to results.txt..."
echo "Daminozide" | python main.py search --index-dir index --document-store data/document_store.jsonl > results.txt

echo "Pipeline finished. Search results are in results.txt"