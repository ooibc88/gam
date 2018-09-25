#!/usr/bin/env bash
bench='/user/caiqc/gam-release/dht/benchmark'
master="ciidaa-a05"
log_dir="/user/caiqc/log"

mk_dat_dir() {
    for ((id = 5; id <= 20; id++)); do
        if ((id < 10)); then
            node="ciidaa-a0"$id
        else
            node="ciidaa-a"$id
        fi
        ssh $node "if [ ! -d $log_dir ]; then mkdir -p $log_dir; fi"
    done
}

kill_all() {
    for ((id = 13; id <= 20; id++)); do
        if ((id < 10)); then
            node="ciidaa-a0"$id
        else
            node="ciidaa-a"$id
        fi

        ssh $node "sudo killall benchmark"
    done
    sleep 1
}

run_client() {
    local nc=$1
    local nt=$2
    local ratio=$3
    local cid=0
    is_master=1
    for ((id = 21 - $nc; id <= 20; id++)); do
        node="ciidaa-a"$id
        log_file="$log_dir/$node"_"$nc"_"$nt"_"$ratio"_"$cid".dat
        if [ "$is_master" -eq 1 ]; then
            master=$node
        fi
        echo "run client at $node  with master $master"
        if [ "$cid" -lt "$(($nc - 1))" ]; then
            cmd="ssh $node \"$bench --is_master $is_master --ip_master $master --ip_worker $node --no_client $nc --get_ratio $ratio --no_thread $nt --client_id $cid 1>$log_file 2>/dev/null &\""
            eval $cmd
            is_master=0
        else
            cmd="ssh $node \"$bench --is_master $is_master --ip_master $master --ip_worker $node --no_client $nc --get_ratio $ratio --no_thread $nt --client_id $cid | tee $log_file \""
            eval $cmd
        fi
        ((cid++))
        sleep 1
    done
}


clients=8
ratios=(100 99 90 50 0)
#mk_dat_dir
for ((thread = 1; thread<=1; thread++)); do
    for ratio in "${ratios[@]}"; do
        kill_all 
        echo "run benchmark with $clients clients $thread threads and $ratio get_ratio "
        run_client $clients $thread $ratio
    done
done
