#!/bin/bash

# Extract the file path from the JSON input
FILE_PATH=$(jq -r '.tool_input.file_path' < /dev/stdin)

# Only format markdown files
if [[ "$FILE_PATH" == *.md ]]; then
  prettier --write "$FILE_PATH" 2>&1
fi

exit 0
