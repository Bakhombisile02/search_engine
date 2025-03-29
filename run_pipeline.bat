@echo off
REM Clean up previous runs
echo Removing existing index and document store...
if exist index rmdir /s /q index
if exist data\document_store.jsonl del /q data\document_store.jsonl
if exist results.txt del /q results.txt

REM Ensure directories exist (use md which doesn't error if exists)
md data > nul 2>&1
md index > nul 2>&1

REM Parse the WSJ XML file
echo Parsing WSJ XML file (data/wsj.xml)...
python main.py parse data/wsj.xml --output-dir data

REM Build the index from the parsed documents
echo Building index (from data/document_store.jsonl)...
python main.py index data/document_store.jsonl --output-dir index

REM Perform search for "Daminozide", piping query via stdin and redirecting results
echo Performing search for 'Daminozide' and outputting to results.txt...
(echo Daminozide) | python main.py search --index-dir index --document-store data/document_store.jsonl > results.txt

echo Pipeline finished. Search results are in results.txt