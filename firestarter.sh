#! /usr/bin/env bash

echo "Framework Overhead Test"

# Block Experiment #

hadoop fs -rmr /user/s0838600/nmr/overhead-test/mini/
#./cluster-runme.sh DefaultTest /user/s0838600/experiments/input/hdfs-blockx15 /user/s0838600/nmr/overhead-test/mini/default/output
./cluster-runme.sh NestingTest /user/s0838600/experiments/input/hdfs-blockx15 /user/s0838600/nmr/overhead-test/mini/nesting/output

# Sort Experiment #

#hadoop fs -rmr /user/s0838600/nmr/sort-test/hellosort/default
#./cluster-runme.sh DefaultOrderSum /user/s0838600/experiments/input/hdfs-blockx20 /user/s0838600/nmr/sort-test/hellosort/default/output
#./cluster-runme.sh NestingOrderSum /user/s0838600/experiments/input/hdfs-blockx5 /user/s0838600/nmr/sort-test/hellosort/nesting/6/output

#./cluster-runme.sh NewNestSort /user/s0838600/experiments/input/hdfs-blockx20 /user/s0838600/nmr/sort-test/hellosort/nesting/alter/10/output

# Sort Experiment #

#hadoop fs -rmr /user/s0838600/nmr/final-test/hellofinal/default
#./cluster-runme.sh DefaultOrderSum /user/s0838600/experiments/input/hdfs-blockx15 /user/s0838600/nmr/final-test/hellofinal/default/output
#./cluster-runme.sh NewNestSort /user/s0838600/experiments/input/hdfs-blockx15 /user/s0838600/nmr/final-test/hellofinal/nesting/9/output
