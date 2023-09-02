#! /usr/bin/env bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
SRC_HOME=$bin
SRC_HOME_155="/home/wpq/gam/fft_gam_parr"
slaves=$bin/slaves
log_file=$bin/log
log_file_155=$SRC_HOME_155/log
master_ip=10.77.110.158
master_port=1237

run() {
	node=2
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
    	echo "$SRC_HOME/fft --no_node $node --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port" | tee -a "$log_file".$port
    	if [ $i = 0 ]; then
			ssh $ip "$SRC_HOME/fft --no_node $node --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port | tee -a '$log_file'.$port" &
		else
			ssh $ip	"$SRC_HOME_155/fft --no_node $node --ip_master $master_ip --ip_worker $ip --port_worker $port --is_master $is_master --port_master $master_port | tee -a '$log_file_155'.$port" &
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