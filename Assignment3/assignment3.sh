#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ASSIGNMENT_PATH=$SCRIPT_DIR"/assignment3.py"

for var in "$@"
do
    echo "$var"
    cat "$var" | parallel --pipe -N1638400 --jobs 4 --slf nodes.txt -k python3 "${ASSIGNMENT_PATH}" |  python3 merge_chunks.py > output.csv
done
