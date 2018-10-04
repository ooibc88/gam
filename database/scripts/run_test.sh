#!/bin/bash
set -o nounset

# specify your hosts_file here 
# hosts_file specify a list of host names and port numbers, with the host names in the first column
hosts_file="../tpcc/config.txt"
# specify your directory for log files
output_dir="/data/wentian"

# working environment
proj_dir="~/programs/gam/code"
bin_dir="${proj_dir}/database/test"
script_dir="{proj_dir}/database/scripts"
ssh_opts="-o StrictHostKeyChecking=no"
bin_file=hash_index_test

hosts_list=`./get_servers.sh ${hosts_file} | tr "\\n" " "`
hosts=(`echo ${hosts_list}`)
master_host=${hosts[0]}

USER_ARGS="$@"
echo "input Arguments: ${USER_ARGS}"
echo "launch..."

run_test () {
  output_file="${output_dir}/${bin_file}.log"
  script="cd ${bin_dir} && ./${bin_file} ${USER_ARGS} > ${output_file} 2>&1"
  
  echo "start master: ssh ${ssh_opts} ${master_host} "$script" &"
  ssh ${ssh_opts} ${master_host} "$script" &
  sleep 3
  for ((i=1;i<${#hosts[@]};i++)); do
    host=${hosts[$i]}
    echo "start worker: ssh ${ssh_opts} ${host} "$script" &"
    ssh ${ssh_opts} ${host} "$script" &
    sleep 1
  done
  wait
}


auto_fill_params () {
  # so that users don't need to specify parameters for themselves
  USER_ARGS="-p11111 -sf32 -sf10 -c4 -t200000 -f../tpcc/config.txt"
}

auto_fill_params
bin_file=hash_index_test
run_test
bin_file=tpcc_populate_test
run_test
