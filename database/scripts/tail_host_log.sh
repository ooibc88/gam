#!/bin/bash
# helper file that can print out the logs of the remote processes

if [ "$#" -ne 3 ]; then
	echo "$0 host_name file_name line_num"
	exit -1
fi

host_name=$1
file_path=$2
line_num=$3

SCRIPT="tail -${line_num} ${file_path}"
SSH_OPTS="-o StrictHostKeyChecking=no"

echo "command: ssh ${SSH_OPTS} ${host_name} "${SCRIPT}""
ssh ${SSH_OPTS} ${host_name} "${SCRIPT}"
