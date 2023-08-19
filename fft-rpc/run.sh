
make build & cd build
cmake ..
make -j
./build/bin/fft --ip_master 10.77.110.155 --ip_worker 10.77.110.227 --port_master 9991 --port_worker 9992 --is_master 1 --rpc_worker 1 --is_run 1


//155
./fft --ip_master 10.77.110.155 --ip_worker 10.77.110.227 --rpc_worker 0 --is_master 1 --is_run 1 --port_master 9996 --port_worker 9997
//227
./fft --ip_master 10.77.110.227 --ip_worker 10.77.110.227 --rpc_worker 1 --is_master 1 --is_run 0 --port_master 9996 --port_worker 9997


//155
./fft --ip_master 10.77.110.158 --ip_worker 10.77.110.155 --rpc_worker 1 --is_master 0 --is_run 0 --port_master 9996 --port_worker 9998
//158
./fft --ip_master 10.77.110.158 --ip_worker 10.77.110.158 --rpc_worker 0 --is_master 1 --is_run 1 --port_master 9996 --port_worker 9997