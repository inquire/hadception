#! /usr/bin/env bash

echo "Fixing cluster install: "
echo "Copying hello.txt from input..."

hadoop dfs -copyFromLocal input/hello.txt /tmp/nestin

echo "Showing the location of the file on HDFS"
