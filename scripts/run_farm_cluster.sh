#!/usr/bin/env bash

master="10.0.1.110"
no_node=10
no_thread=4
write_ratio=50
num_obj=100000
obj_size=100
iteration=10000
txn_nobj=40

exec="/users/caiqc/research/GlobalMemory/code/test/farm_cluster_test --ip_master $master --no_node $no_node --no_thread $no_thread --write_ratio $write_ratio --num_obj $num_obj --obj_size $obj_size --iteration $iteration --txn_nobj $txn_nobj"

for (( i = 0; i < no_node; i++)); do
    worker="10.0.0.$((i + 110))"
    log="/users/caiqc/gam/code/log/farm-$worker"".log"
    let node_id=i
    if [ $i -eq 0 ]; then
        is_master=1
    else
        is_master=0
    fi

    cmd="$exec --ip_worker $worker --node_id $node_id --is_master $is_master"

    ssh $worker "$cmd 1>$log 2>$log &"

    if [ $i -eq 0 ]; then
        sleep 1;
    fi
done
