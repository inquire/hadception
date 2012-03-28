#! /usr/bin/env bash

#./cleanup.sh

echo "Cleaning up hdfs structure"
hadoop dfs -rmr /tmp/outputs
hadoop dfs -rmr /tmp/inceptions
hadoop dfs -rmr /tmp/outception
hadoop dfs -rmr /tmp/outceptions

echo " "

echo "Removing inception directory..."
rm -rf inception
rm -rf framejar
echo "Done"

echo " "

echo "Creating new class directorys..."
mkdir inception
mkdir framejar
echo "Done"

echo "Compiling the framework classes..."

#javac -Xlint -classpath $HADOOP_HOME/hadoop-core-0.20.204.0.jar:$HADOOP_HOME/lib/commons-cli-1.2.jar -d framejar src/inf/mapred/*.java

echo "Creating the framework jar file..."

#jar cvf mapred-framework.jar -C framejar .



echo "Compiling the classes into inception..."
javac -Xlint -classpath $HADOOP_HOME/hadoop-core-0.20.204.0.jar:$HADOOP_HOME/lib/commons-cli-1.2.jar -d inception src/*.java

echo "Creating primary jar file..." 
jar cvf inception.jar -C inception .  


#echo 'Erasing common jar from hdfs...'
#hadoop fs -rmr /tmp/utils

#echo 'Adding jar to hdfs...'
#hadoop fs -mkdir /tmp/utils
#hadoop fs -copyFromLocal modword.jar /tmp/utils

echo "Starting job on cluster!"
#hadoop jar inception.jar MRMain -libjars inception.jar /tmp/nestin /tmp/outputs/1
hadoop jar inception.jar MRMain -libjars inception.jar /tmp/smaller /tmp/outputs/1
