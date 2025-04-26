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
intermediary_file="${input_file}.cleaned"
output_file="${intermediary_file}.hashed"


# Ensure output file is empty before writing
> "$intermediary_file"
> "$output_file"

grep -oE '\b[0-9]{11}\b' "$input_file" > "$intermediary_file"

# Read each line, trim leading and trailing spaces, hash it, and save to the output file
while IFS= read -r line; do
    trimmed_line=$(echo "$line" | awk '{$1=$1; print}') #Trim spaces using awk
    #trimmed_line=$(echo "$line" | sed 's/^[ \t]*//;s/[ \t]*$//')  # Trim spaces using sed
     # Check if the line has exactly 11 digits
    if echo "$trimmed_line" | grep -qE '^[0-9]{11}$'; then
        echo -n "$trimmed_line" | sha512sum | awk '{print $1}' >> "$output_file"
    fi
done < "$intermediary_file"

echo "Hashes saved in $output_file"
