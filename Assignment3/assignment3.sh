#!/bin/bash

echo "$1"
# TO DO: See if you can make this for multiple files
cat "$1" | parallel --pipe -N8 "python3 assignment3.py"|  python3 merge_chunks.py
