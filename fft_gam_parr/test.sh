#! /usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin
SRC_HOME_155="/home/wpq/gam/fft_gam_parr"
SRC_HOME_141="/home/wupuqing/gam/fft_gam_parr"
SRC_HOME_227="/home/wpq/wpq-gam/gam/fft_gam_parr"
SRC_HOME_161="/home/wpq/wpq-gam/gam/fft_gam_parr"
SRC_HOME_158="/home/wpq/rdmatest/gam/fft_gam_parr"
slaves=$bin/slaves
log_file=$bin/log
log_file_155=$SRC_HOME_155/log
log_file_141=$SRC_HOME_141/log
log_file_227=$SRC_HOME_227/log
log_file_161=$SRC_HOME_161/log
log_file_158=$SRC_HOME_158/log
master_ip=10.77.110.155
master_port=1237

run() {
	node=3
	no_run=2
	is_read=1
	is_sync=0
	see_time=1
	sleep_time=1
    old_IFS=$IFS
    IFS=$'\n'
    i=0
    for slave in `cat "$slaves"`
    do
    	ip=`echo $slave | cut -d ' ' -f1`
    	port=`echo $slave | cut -d ' ' -f2`
    	if [ $i = 0 ]; then
    		is_master=1
            master_ip=$ip
    	else
    		is_master=0
    	fi
    	if [ $port == $ip ]; then
    		port=1234
    	fi
    	echo ""
    	echo "slave = $slave, ip = $ip, port = $port"
    	echo "$SRC_HOME/fft --no_node $node --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read --is_sync $is_sync --see_time $see_time --sleep_time $sleep_time" | tee -a "$log_file".$port
    	if [ $i = 0 ]; then
			ssh $ip "$SRC_HOME/fft --no_node $node --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read --is_sync $is_sync --see_time $see_time --sleep_time $sleep_time | tee -a '$log_file'.$port" &
		elif [ $i = 1 ]; then
			ssh -p 5102 wupuqing@$ip	"$SRC_HOME_141/fft --no_node $node --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read --is_sync $is_sync --see_time $see_time --sleep_time $sleep_time | tee -a '$log_file_141'.$port" &
			# sshpass -p "wpqlab505" ssh $ip	"$SRC_HOME_161/fft --no_node $node --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port | tee -a '$log_file_161'.$port" &
		else 
			ssh wpq@$ip "$SRC_HOME_158/fft --no_node $node --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port --no_run $no_run --is_read $is_read --is_sync $is_sync --see_time $see_time --sleep_time $sleep_time | tee -a '$log_file_158'.$port" &
			
		fi
		sleep 1
    	i=$((i+1))
    	if [ "$i" = "$node" ]; then
    		break
    	fi
    done # for slave
	wait

    IFS="$old_IFS"
}

run

pkill fft  