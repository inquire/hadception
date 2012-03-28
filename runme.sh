#! /usr/bin/env bash

echo "Clean directory..."
rm -rf scratch
rm inception.jar
rm nmr-eames.jar

echo "Clean HDFS..."
hadoop fs -rmr /tmp/inceptions
hadoop fs -rmr /tmp/outputs



echo "Generate computation steps in scratch."
mkdir scratch
cd scratch

# Framework is compiled into buildbin
echo "Compile framework into scratch."
mkdir buildbin

javac -d -Xlint -classpath $HADOOP_HOME/hadoop-core-0.20.204.0.jar:$HADOOP_HOME/lib/commons-cli-1.2.jar -d buildbin ../framework/uk/ac/ed/inf/nmr/*/*.java
echo "Compiling the classes into a jar!"
ls -Rl buildbin

echo " "

echo "Compile classes into jar"
jar cvf nmr-eames.jar -C buildbin .

echo " "
echo " ================== Compile User Code with NMR ================== "
echo " "

ls -l

echo "Composing layer-2"
mkdir layer-2
mkdir layer-1

javac -Xlint -classpath $HADOOP_HOME/hadoop-core-0.20.204.0.jar:$HADOOP_HOME/lib/commons-cli-1.2.jar:./nmr-eames.jar -d layer-2 ../src/*.java
mv nmr-eames.jar ../

cp -r buildbin/uk layer-2
mkdir layer-2/lib
ls -l layer-2

echo " "
echo " ================== Moving all code in layer-1 ================== "
echo " "

cp -r layer-2/* layer-1/
jar cvf inception.jar -C layer-2 .
mv inception.jar layer-1/lib/
ls -Rl layer-1

echo " "
echo " ================== JARing code and ship it ================== "
echo " "

jar cvf inception.jar -C layer-1 .
mv inception.jar ../
cd ../
ls -l scratch/layer-1

echo " "
echo " ================== Submitted to cluster ! ================== "
echo " "

hadoop jar inception.jar MRMain -D mapred.reduce.tasks=1 /tmp/nestin /tmp/outputs


