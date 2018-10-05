# Overview

GAM (Globally Addressable Memory) is a distributed memory management platform
which provides a global, unified memory space over a cluster of nodes connected
via RDMA (Remote Direct Memory Access).  GAM allows nodes to employ a cache to
exploit the locality in global memory accesses, and uses an RDMA-based,
distributed cache coherency protocol to keep cached data consistent.  Unlike
existing distributed memory management systems which typically employ Release
Consistency and require synchronization primitives to be explicitly called for
data consistency, GAM enforces the PSO (Partial Store Order) memory model which
ensures data consistency automatically and relaxes the Read-After-Write and
Write-After-Write ordering to remove costly writes from critical program
execution paths. For more information, please refer to our [VLDB'18
paper](#paper).

# Build & Usage
## Prerequisite
1. `libverbs`
2. `boost thread`
3. `boost system`
4. `gcc 4.8.4+`

## GAM Core
First build `libcuckoo` in the `lib/libcuckoo` directory by following the
`README.md` file in that directory, and then go to the `src` directory and run `make`
therein.
```
  cd src;
  make -j;
```

### Test and Micro Benchmark
We provide an extensive set of tools to test and benchmark GAM. These tools are
contained in the `test` directory, and also serve the purpose of demonstrating
the usage of the APIs provided in GAM. To build them, simply run `make -j` in
the `test` directory.

A script `benchmark-all.sh` is provided in the `script` directory to facilitate
the benchmarking of GAM. This script is also used to generate the result of the
micro benchmark in the [GAM paper](#paper). To run this script, a `slaves` file needs to
be provided within the same directory. Each line of the `slaves` file contains
the ip address and port (separated by space) of a node that is involved in the
benchmarking, and the number of lines contained in the `slaves` file should
be no smaller than that of nodes for benchmarking.
There are multiple parameters that can be varied for
a thorough benchmarking, please refer to [our paper](#paper) for detail.

## Applications
We build two distributed applications on top of GAM by using the APIs GAM
provide, a distributed key-value store and distributed transaction processing
engine. To build them, simply run the below commands:
```
  cd dht
  make -j
  cd ../database
  make -j
```

### Macro Benchmark
There is a script `kv-benchmark.sh` provided in the `dht` directory to
benchmark the key-value store. To run it, please change the variables in the
script according to the experimental setting. There are also several parameters
that can be varied for benchmarking, such as thread number, get ratio and
number of nodes. Please refer to the [GAM paper](#paper) and the script for detail. 

To run the TPCC benchmark, please follow the instructions of the `README` file
in the `database` directory.

## FaRM
We implement the [FaRM](#farm) system as a baseline for macro benchmark. To build
the FaRM codebase, please run the below command:
```
  git checkout farm 
  cd src
  make -j
```

We also provide several tools to test and benchmark our FaRM implementation.
Please go to the `test` directory, and `make -j` therein to generate those
tools. All tools but `farm-cluster-test` can be run directly. For
`farm-cluster-test`, a script `run_farm_cluster.sh` is provided in `scripts`
directory. Please change the variables in that script according to the
deployment environment.

# References
<a name="paper"></a>
[1] Qingchao Cai, Wentian Guo, Hao Zhang, Gang Chen, Beng Chin Ooi, Kian-Lee Tan, Yong Meng Teo, and Sheng Wang. *Efficient Distributed Memory Management with RDMA and Caching*. PVLDB, 11 (11): 1604- 1617, 2018. DOI: https://doi.org/10.14778/3236187.3236209.

<a name="farm"></a>
[2] Aleksandar Dragojević, Dushyanth Narayanan, Orion Hodson, and Miguel Castro. *FaRM: Fast remote memory*. Proceedings of the 11th USENIX Conference on Networked Systems Design and Implementation. 2014.
