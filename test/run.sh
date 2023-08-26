#!/bin/sh
cd ../src
make clean
make -j
cd ../test
make clean
make test_matrix
./test_matrix

