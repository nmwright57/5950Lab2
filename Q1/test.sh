#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /spark-examples/lab2/Question1/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /spark-examples/lab2/Question1/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal /spark-examples/lab2/shot_logs.csv /spark-examples/lab2/Question1/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./nba-kmeans-q1.py hdfs://$SPARK_MASTER:9000/spark-examples/lab2/Question1/input/