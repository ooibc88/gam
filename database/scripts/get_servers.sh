#!/bin/bash
# Print out the first column of a file

if [ $# -ne 1 ]; then
  echo "USAGE: ./get_servers.h INPUT_FILE"
fi

input_file=$1
awk '{print $1}' ${input_file}
