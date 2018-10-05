#!/bin/bash
# This script extract the running results of tpcc benchmark and write to OUTFILE
OUTFILE=result.out
INPUTDIR=/data/wentian
INFILE=tpcc.log

echo "" > $OUTFILE
DIST_RATIO_ARRAY=(0 10 20 30 40 50 60 70 80 90 100)
for DIST_RATIO in ${DIST_RATIO_ARRAY[@]}; do
    INPUT=$INPUTDIR/${DIST_RATIO}_$INFILE
    echo "INPUT=$INPUT"
    awk 'BEGIN {throughput=0; abort_rate=0;}
    /total_throughput/{throughput=$2; }
    /abort_rate/{abort_rate=$2; }
    END {print throughput,"\t", abort_rate; }' $INPUT >> $OUTFILE
done
echo "done"
