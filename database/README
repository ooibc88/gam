A transactional database engine built on top of Global addressable memory. 
This engine is benchmarked with TPCC.

# Feature #
Since GAM maintains the cache coherence, i.e., all records to access would be cached locally before the accesses, two phase commit is not needed and the transactions are executed similar to the case of single machines.
Therefore, we use two phase locking protocol for concurrency control

# Compile #
It is required that the GAM library is built (in 'code/src').

To build the tpcc benchmark, just
```
cd tpcc; make clean; make -j
```
To build the test for tpcc, simply
```
cd test; make clean; make -j
```
The file 'scripts/compile.sh' automate this compilation process.
After compilation, the binary files are generated in the folder 'tpcc' and 'test' respectively.

# Flag #
There are two flags used for compilation to enforce different concurrency control options.
* LOCK: two phase locking
* ST: no concurrency control

# Run #
1. Specify the cluster setup in a configuration file, say 'config.txt'. 
This file should be written in the format as a number of lines of "HOST_NAME PORT_NUMBER". 

Note that the master node should be written in the first line.
The file 'tpcc/config.txt' provides an example for such a configuration file.

2. Start the master node with the specified arguments.

3. Start the remaining nodes with the specified arguments.


The arguments for 'tpcc' are described as follows.
* -p: the port number for this node (required).
* -c: Number of cores (threads) used (required).
* -sf: the scale factors for populating the tpcc benchmark. 

There are two arguments for '-sf'.
The first argument for '-sf' indicates the number of warehouses, while the second argument for '-sf' is to scale the size of the database. 

* -t: Number of transactions run for each thread
* -d: The distributed ratios.
* -f: The configuration file for the cluster setup
* -r: The read ratio, used to modify the workload
* -l: The time locality, used to modify the workload

You can also simply run 'tpcc' without any arguments to show the description for each argument.

The folder 'scripts' automate the runnning of tpcc benchmark. 

Under this folder, 'experiment.sh' can run the benchmark under varying distributed ratios, read ratios, and time locality;
'run_test.sh' can automate the running of our tests.

Notes: To have a large global memory space, you may need to modify size in 'include/struture.h'. 


# Acknowledgement #
This implementation is adapted from an open-source transactional database prototype on single machines:
https://github.com/Cavalia/Cavalia
