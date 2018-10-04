#!/bin/bash
set -o nounset

# kill all processes located in HOSTS_FILE

# specify your hosts_file here 
hosts_file="../tpcc/config.txt"

hosts_list=`./get_servers.sh ${hosts_file} | tr "\\n" " "`
HOSTS=(`echo ${hosts_list}`)

APP_NAME="tpcc"
#APP_NAME="hash_index_test"
if [ $# -eq 1 ]; then
  APP_NAME="$1"
fi
SCRIPT="pkill -f ${APP_NAME}"
SSH_OPTS="-o StrictHostKeyChecking=no"

#echo "${HOSTS[@]}"
#echo ${APP_NAME}""
printf "kill "
for HOST_NAME in ${HOSTS[@]} ; do
    echo "ssh ${SSH_OPTS} ${HOST_NAME} ${SCRIPT}"
    ssh ${SSH_OPTS} ${HOST_NAME} "${SCRIPT}"
    printf "."
done
echo "done"

