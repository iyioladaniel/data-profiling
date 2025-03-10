#!/bin/bash

# This script reads a file line by line, hashes each line using sha 512, and keeps the hashed value in a new file.
# Usage: hash-ids.sh
# code by Daniel Iyiola

# Check if a file argument is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <input_file>"
    exit 1
fi

input_file="$1"
output_file="${input_file}.hashed"

# Ensure output file is empty before writing
> "$output_file"

# Read each line, trim leading and trailing spaces, hash it, and save to the output file
while IFS= read -r line; do
    trimmed_line=$(echo "$line" | awk '{$1=$1; print}')
    echo -n "$trimmed_line" | sha512sum | awk '{print $1}' >> "$output_file"
done < "$input_file"

echo "Hashes saved in $output_file"
